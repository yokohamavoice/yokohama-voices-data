import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash,randomUUID} from 'node:crypto';

// Shared verifier: copied into the independent management script as well.
const TABLES=['opinions','responses','presentations'];
const EXPECTED={agree:-1,disagree:1,pass:0,unrelated:null};
const UUID=/^[a-f0-9-]{36}$/;
const SHA=/^[a-f0-9]{64}$/;
const rawRoots=new WeakMap();
export const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const plain=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const exact=(value,names)=>plain(value)&&Object.keys(value).sort().join(',')===[...names].sort().join(',');
const integer=value=>Number.isSafeInteger(value)&&value>=0;
const text=value=>typeof value==='string'&&value.length>0;
function fail(message){throw new Error(message);}
function decode(bytes){return new TextDecoder('utf-8',{fatal:true}).decode(bytes);}
export function validateRef(ref,table,kind){
  if(!exact(ref,['part','sha256','row_count','byte_length'])||!new RegExp(`^${kind}-${table}-(?:[0-9]{9}|[0-9]{12})$`).test(ref.part)||!SHA.test(ref.sha256)||!integer(ref.row_count)||!integer(ref.byte_length))fail('Invalid page reference');
  return ref;
}
function validateMetadata(data){
  if(!exact(data.release,['id','prepared_at','cutoff','routing_version','description'])||!UUID.test(data.release.id)||!['prepared_at','cutoff','routing_version','description'].every(k=>typeof data.release[k]==='string')||typeof data.updated_through!=='string'||!Array.isArray(data.tags)||!Array.isArray(data.sources))fail('Invalid release metadata');
  if(!data.tags.every(t=>plain(t)&&text(t.id))||new Set(data.tags.map(t=>t.id)).size!==data.tags.length)fail('Invalid tags');
}
function validateRow(table,row,tagIds,opinionIds){
  if(table==='opinions'){
    if(!exact(row,['id','tag_id','text','kind','source_ids','created_date'])||!text(row.id)||typeof row.text!=='string'||!tagIds.has(row.tag_id)||!['seed','participant'].includes(row.kind)||!Array.isArray(row.source_ids)||!row.source_ids.every(x=>typeof x==='string')||!(row.created_date===null||/^\d{4}-\d{2}-\d{2}$/.test(row.created_date)))fail('Invalid public opinion');
    if(opinionIds.has(row.id))fail('Duplicate opinion');opinionIds.add(row.id);
    return row.id;
  }
  if(table==='responses'){
    if(!exact(row,['session_id','opinion_id','response','analysis_value'])||!text(row.session_id)||!opinionIds.has(row.opinion_id)||!Object.hasOwn(EXPECTED,row.response)||EXPECTED[row.response]!==row.analysis_value)fail('Invalid public response');
    return JSON.stringify([row.session_id,row.opinion_id]);
  }
  if(!exact(row,['id','session_id','opinion_id','sequence','probability','routing_version','displayed','answered','retired'])||!text(row.id)||!text(row.session_id)||!opinionIds.has(row.opinion_id)||!Number.isSafeInteger(row.sequence)||row.sequence<1||!Number.isFinite(row.probability)||!(row.probability>0&&row.probability<=1.00000000001)||!text(row.routing_version)||!['displayed','answered','retired'].every(k=>typeof row[k]==='boolean'))fail('Invalid public presentation');
  return row.id;
}
export function validate(data){
  validateMetadata(data);
  if(data.schema_version===2){
    if(!exact(data,['schema_version','release','updated_through','tags','sources',...TABLES])||!TABLES.every(k=>Array.isArray(data[k])))fail('Invalid version 2 root');
    const tagIds=new Set(data.tags.map(t=>t.id)),opinionIds=new Set();
    for(const table of TABLES){const seen=new Set();for(const row of data[table]){const id=validateRow(table,row,tagIds,opinionIds);if(seen.has(id))fail('Duplicate public row');seen.add(id);}}
    return data;
  }
  if(data.schema_version!==3||data.format!=='jsonl-pages-v1'||!exact(data,['schema_version','format','release','updated_through','tags','sources','tables'])||!exact(data.tables,TABLES))fail('Invalid version 3 root');
  for(const table of TABLES){const entry=data.tables[table];if(!exact(entry,['rows','pages','index'])||!integer(entry.rows)||!integer(entry.pages))fail('Invalid table summary');if(entry.index===null){if(entry.rows!==0||entry.pages!==0)fail('Missing table index');}else{validateRef(entry.index,table,'i');if(!entry.rows||!entry.pages)fail('Unexpected empty table index');}}
  return data;
}
export async function responseBytes(response,expected){
  if(!response.ok)fail(`HTTP ${response.status}`);
  const bytes=Buffer.from(await response.arrayBuffer()),checksum=hash(bytes);
  if(response.headers.get('x-content-sha256')!==checksum||expected&&checksum!==expected)fail('Dataset hash mismatch');
  return bytes;
}
async function atomic(file,bytes){await fs.mkdir(path.dirname(file),{recursive:true});const temp=file+'.tmp-'+randomUUID();try{await fs.writeFile(temp,bytes);await fs.rename(temp,file);}finally{await fs.rm(temp,{force:true});}}
async function immutable(file,bytes){try{const old=await fs.readFile(file);if(!old.equals(bytes))fail('Published release changed under the same ID');return;}catch(e){if(e.code!=='ENOENT')throw e;}await atomic(file,bytes);}
const partFilename=part=>part+(part.startsWith('i-')?'.json':'.jsonl');
/** Verify and save each bounded page; never concatenate a v3 table in memory. */
export async function archiveDataset(data,{directory,loadPart,rootBytes=rawRoots.get(data)||Buffer.from(JSON.stringify(data))}){
  validate(data);const rootHash=hash(rootBytes);
  // An existing root must match even while resuming a partly downloaded archive.
  try{if(hash(await fs.readFile(path.join(directory,'dataset.json')))!==rootHash)fail('Archived release root changed');}catch(e){if(e.code!=='ENOENT')throw e;}
  if(data.schema_version===2){await immutable(path.join(directory,'dataset.json'),rootBytes);return {schema_version:2,release_id:data.release.id,sha256:rootHash,counts:Object.fromEntries(TABLES.map(t=>[t,data[t].length])),complete:true};}
  if(typeof loadPart!=='function')fail('Paged dataset requires a page loader');
  const tagIds=new Set(data.tags.map(t=>t.id)),opinionIds=new Set(),counts={};
  async function obtain(ref,table,kind){
    validateRef(ref,table,kind);const file=path.join(directory,'parts',partFilename(ref.part));let bytes;
    try{bytes=await fs.readFile(file);}catch(e){if(e.code!=='ENOENT')throw e;bytes=Buffer.from(await loadPart(ref));}
    if(bytes.length!==ref.byte_length||hash(bytes)!==ref.sha256)fail('Page length/hash mismatch');
    return {bytes,file};
  }
  for(const table of TABLES){
    const summary=data.tables[table];let index=summary.index,rows=0,pages=0;const visited=new Set();
    while(index!==null){
      if(visited.has(index.part))fail('Cyclic page index');visited.add(index.part);
      const current=await obtain(index,table,'i'),node=JSON.parse(decode(current.bytes));
      if(!exact(node,['schema_version','kind','release_id','table','pages','next'])||node.schema_version!==3||node.kind!=='page_index'||node.release_id!==data.release.id||node.table!==table||!Array.isArray(node.pages)||node.pages.length<1||node.pages.length>256||node.pages.length!==index.row_count)fail('Invalid page index');
      if(node.next!==null)validateRef(node.next,table,'i');
      for(const ref of node.pages){
        validateRef(ref,table,'d');if(visited.has(ref.part))fail('Repeated data page');visited.add(ref.part);
        const part=await obtain(ref,table,'d');const content=decode(part.bytes),lines=content.split('\n');if(lines.at(-1)==='')lines.pop();
        if(lines.some(line=>!line.trim())||lines.length!==ref.row_count)fail('JSONL row count mismatch');
        const seen=new Set();for(const line of lines){const id=validateRow(table,JSON.parse(line),tagIds,opinionIds);if(seen.has(id))fail('Duplicate row within page');seen.add(id);}
        rows+=lines.length;pages++;if(rows>summary.rows||pages>summary.pages)fail('Page totals exceed root');await immutable(part.file,part.bytes);
      }
      await immutable(current.file,current.bytes);index=node.next;
    }
    if(rows!==summary.rows||pages!==summary.pages)fail('Incomplete table');counts[table]=rows;
  }
  await immutable(path.join(directory,'dataset.json'),rootBytes);
  const manifest={schema_version:3,format:'jsonl-pages-v1',release_id:data.release.id,sha256:rootHash,updated_through:data.updated_through,counts,tables:data.tables,complete:true};
  await immutable(path.join(directory,'manifest.json'),Buffer.from(JSON.stringify(manifest,null,2)+'\n'));
  return manifest;
}
// End shared verifier.

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
import {workflowConfiguration} from './workflow-config.mjs';
export function syncConfiguration(env=process.env){const config=workflowConfiguration(env,'public');return {...config,endpoint:config.origin+'/api/export'};}
async function fetchRetry(url){for(let attempt=0;attempt<3;attempt++){try{const response=await fetch(url,{signal:AbortSignal.timeout(30000),headers:{Accept:'application/json, application/x-ndjson'}});if(response.status>=500&&attempt<2)throw new Error(`Export HTTP ${response.status}`);return response;}catch(error){if(attempt===2)throw error;await new Promise(resolve=>setTimeout(resolve,1000*(attempt+1)));}}}
export async function fetchData(releaseId,config=syncConfiguration()){
  const {endpoint}=config;
  const response=await fetchRetry(endpoint+(releaseId?`?release=${encodeURIComponent(releaseId)}`:''));
  if(response.status===404){const data=await response.json();if(data.status==='awaiting_review')return null;fail('Published release not found');}
  const bytes=await responseBytes(response),data=validate(JSON.parse(decode(bytes)));rawRoots.set(data,bytes);return data;
}
export async function save(data,output=path.join(root,'data'),archive=path.join(root,'releases'),options={}){
  if(!data)return false;validate(data);const raw=rawRoots.get(data)||Buffer.from(JSON.stringify(data)),checksum=hash(raw),directory=path.join(archive,data.release.id);
  const source=options.source||null;
  const loadPart=options.loadPart||((ref)=>fetchRetry(syncConfiguration().endpoint+`?release=${encodeURIComponent(data.release.id)}&part=${encodeURIComponent(ref.part)}`).then(r=>responseBytes(r,ref.sha256)));
  const archived=await archiveDataset(data,{directory,loadPart,rootBytes:raw});
  let previous;try{previous=JSON.parse(await fs.readFile(path.join(output,'manifest.json'),'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
  if(previous?.release_id===data.release.id&&previous?.sha256===checksum){console.log('Approved public data unchanged.');return false;}
  await fs.mkdir(output,{recursive:true});
  await atomic(path.join(output,'tags.json'),JSON.stringify(data.tags,null,2)+'\n');await atomic(path.join(output,'sources.json'),JSON.stringify(data.sources,null,2)+'\n');
  let manifest;
  if(data.schema_version===2){
    const keys={opinions:o=>o.id,responses:r=>r.session_id+'\t'+r.opinion_id,presentations:p=>p.session_id+'\t'+String(p.sequence).padStart(10,'0')};
    for(const table of TABLES){const content=[...data[table]].sort((a,b)=>keys[table](a).localeCompare(keys[table](b),'en')).map(r=>JSON.stringify(r)).join('\n')+(data[table].length?'\n':'');await atomic(path.join(output,table+'.jsonl'),content);}
    manifest={...archived,source,updated_through:data.updated_through,snapshot_saved_at:new Date().toISOString(),counts:{...archived.counts,sessions:new Set(data.responses.map(r=>r.session_id)).size},files:[...TABLES.map(t=>t+'.jsonl'),'tags.json','sources.json']};
    const archivedManifest=path.join(directory,'manifest.json');
    try{const old=JSON.parse(await fs.readFile(archivedManifest,'utf8'));if(old.release_id!==manifest.release_id||old.sha256!==manifest.sha256)fail('Archived manifest changed');}catch(error){if(error.code!=='ENOENT')throw error;await immutable(archivedManifest,Buffer.from(JSON.stringify(manifest,null,2)+'\n'));}
  }else manifest={...archived,source,dataset_path:path.relative(output,path.join(directory,'dataset.json')).split(path.sep).join('/'),files:['tags.json','sources.json']};
  await atomic(path.join(output,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
  if(data.schema_version===3)for(const table of TABLES)await fs.rm(path.join(output,table+'.jsonl'),{force:true});
  console.log(`Saved approved release ${data.release.id} (schema ${data.schema_version}).`);return true;
}
export async function syncAll({env=process.env}={}){
  const config=syncConfiguration(env),{endpoint}=config;
  const response=await fetchRetry(endpoint+'?list=1');if(!response.ok)fail(`Release index HTTP ${response.status}`);const index=await response.json();if(!Array.isArray(index.releases))fail('Invalid release list');
  for(let i=0;i<index.releases.length;i++){
    const release=index.releases[i];if(!UUID.test(release.id)||!SHA.test(release.sha256))fail('Invalid release metadata');
    const directory=path.join(root,'releases',release.id);let existing=false;
    try{const bytes=await fs.readFile(path.join(directory,'dataset.json'));if(hash(bytes)!==release.sha256)fail('Archived release changed');const manifest=JSON.parse(await fs.readFile(path.join(directory,'manifest.json'),'utf8'));existing=manifest.schema_version===2||manifest.complete===true;}catch(e){if(e.code!=='ENOENT')throw e;}
    if(!existing||i===index.releases.length-1){const data=await fetchData(release.id,config);if(!data||hash(rawRoots.get(data))!==release.sha256)fail('Release index and root disagree');await save(data,undefined,undefined,{source:endpoint,loadPart:ref=>fetchRetry(endpoint+`?release=${encodeURIComponent(release.id)}&part=${encodeURIComponent(ref.part)}`).then(r=>responseBytes(r,ref.sha256))});}
  }
  if(!index.releases.length)console.log('No approved releases yet; existing history retained.');
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))await syncAll();
