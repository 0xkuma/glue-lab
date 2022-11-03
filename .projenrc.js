const { awscdk } = require('projen');
const project = new awscdk.AwsCdkTypeScriptApp({
  cdkVersion: '2.50.0',
  defaultReleaseBranch: 'main',
  name: 'glue-lab',
  workflowGitIdentity: false,
  github: false,
  gitignore: ['.env'],
  deps: ['dotenv'], /* Runtime dependencies of this module. */
  tsconfig: {
    compilerOptions: {
      lib: ['es2020'],
      target: 'es2020',
    },
  },
  // description: undefined,  /* The description is just a string that helps people understand the purpose of the package. */
  // devDeps: [],             /* Build dependencies for this module. */
  // packageName: undefined,  /* The "name" in package.json. */
});
project.synth();