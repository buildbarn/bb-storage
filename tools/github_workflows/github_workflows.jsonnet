local workflows_template = import 'tools/github_workflows/workflows_template.libsonnet';

workflows_template.getWorkflows(
  ['bb_replicator', 'bb_storage'],
  ['bb_replicator:bb_replicator', 'bb_storage:bb_storage'],
)
