// Empty settings keep these workflows disconnected until the project is ready.
function fail(message){throw new Error(message);}
export function workflowConfiguration(env,kind){
  const enabled=kind==='moderation'?'MODERATION_ENABLED':'PUBLIC_SYNC_ENABLED';
  const repositoryKey=kind==='moderation'?'MODERATION_REPOSITORY':'PUBLIC_DATA_REPOSITORY';
  if(env[enabled]!=='true')fail('Project workflow is disabled');
  const repository=env[repositoryKey],operator=env.PROJECT_OPERATOR_LOGIN;
  if(!/^[a-zA-Z0-9-]+\/[a-zA-Z0-9_.-]+$/.test(repository||''))fail('Set the project repository');
  if(!/^[a-zA-Z0-9-]+$/.test(operator||''))fail('Set the project operator account');
  if(env.GITHUB_REPOSITORY!==repository||env.GITHUB_REF!=='refs/heads/main')fail('Unexpected workflow repository or branch');
  if(env.GITHUB_ACTOR!==operator||env.GITHUB_TRIGGERING_ACTOR!==operator)fail('Only the configured project operator may run this workflow');
  let url;try{url=new URL(env.SITE_ORIGIN);}catch{fail('Set the project site origin');}
  if(url.protocol!=='https:'||url.username||url.password||url.pathname!=='/'||url.search||url.hash||url.port)fail('Use an HTTPS site origin without a path');
  if(!/^\d+$/.test(env.GITHUB_RUN_ID||''))fail('Invalid workflow run ID');
  return {repository,origin:url.origin,creator:'すすすす'};
}
