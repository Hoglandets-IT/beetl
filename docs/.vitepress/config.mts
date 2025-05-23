import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

const config = defineConfig({
  title: "BeETL",
  description: "Data transportation and transformation software",
  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Getting Started', link: '/getting-started' },
      { text: 'API Reference', link: '/api-reference' }
    ],

    sidebar: [
      {
        text: 'General',
        items: [
          { text: 'Quickstart', link: '/getting-started' },
          { text: 'Diff tracking', link: '/getting-started/diff-tracking' },
          { text: 'Column Specification', link: '/getting-started/columns' },
          { text: 'The Flow', link: '/getting-started/flow' },
          { text: 'Change Notes', link: '/getting-started/change-notes' },
        ]
      },
      {
        text: 'Examples',
        items: [
          { text: 'Nutanix to SQL Server', link: '/examples/nutanix-to-sqlserver' },
          { text: 'SqlServer to iTop', link: '/examples/sqlserver-to-itop' },
        ]
      },
      {
        text: 'Schema specification',
        items: [
          { text: 'Configuration', link: '/schemas/configuration' },
        ]
      },
      {
        text: 'Sources',
        items: [
          { text: 'Csv', link: '/sources/csv' },
          { text: 'Excel', link: '/sources/excel' },
          { text: 'Faker', link: '/sources/faker' },
          { text: 'iTop', link: '/sources/itop' },
          { text: 'MongoDB', link: '/sources/mongodb' },
          { text: 'MySQL', link: '/sources/mysql' },
          { text: 'Postgres', link: '/sources/postgres' },
          { text: 'Rest API', link: '/sources/rest' },
          { text: 'Static', link: '/sources/static' },
          { text: 'SQL Server', link: '/sources/sqlserver' },
          { text: 'Xml', link: '/sources/xml' },
        ]
      },
      {
        text: 'Transformers',
        items: [
          { text: 'Using Transformers', link: '/transformers/using-transformers' },
          { text: 'Frames', link: '/transformers/frames' },
          { text: 'Integers', link: '/transformers/int' },
          { text: 'iTop', link: '/transformers/itop' },
          { text: 'Miscellaneous', link: '/transformers/misc' },
          { text: 'Regex', link: '/transformers/regex' },
          { text: 'Strings', link: '/transformers/strings' },
          { text: 'Structs', link: '/transformers/structs' },
        ]
      },
      {
        text: 'Developer Reference',
        items: [
          { text: 'Extending BeETL', link: '/developer-reference/extending' },
          { text: 'Creating a Source', link: '/developer-reference/creating-source' },
          { text: 'Creating a Transformer', link: '/developer-reference/creating-transformer' },
          { text: 'Submitting improvements', link: '/developer-reference/improvements' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/hoglandets-it/beetl' }
    ]
  }
})

const configWithMermaid = withMermaid(config)

export default configWithMermaid
