const fs = require('fs');
const path = require('path');

const siteRoot = path.resolve(__dirname, '..');
const sourcePath = path.join(siteRoot, 'APIs.md');
const outputDir = path.join(siteRoot, 'src', 'pages');
const outputPath = path.join(outputDir, 'APIs.jsx');

const apiReferenceHtml = fs
  .readFileSync(sourcePath, 'utf8')
  .replace(/<\/br>/g, '<br />')
  .replace(/<br>/g, '<br />')
  .replace(/\\\[/g, '[')
  .replace(/\\\]/g, ']');

const page = `import Layout from '@theme/Layout';

const apiReferenceHtml = ${JSON.stringify(apiReferenceHtml)};

export default function APIs() {
  return (
    <Layout title="APIs" description="Numaflow API reference">
      <main className="container margin-vert--lg api-reference">
        <h1>APIs</h1>
        <div dangerouslySetInnerHTML={{__html: apiReferenceHtml}} />
      </main>
    </Layout>
  );
}
`;

fs.mkdirSync(outputDir, {recursive: true});
fs.writeFileSync(outputPath, page);
