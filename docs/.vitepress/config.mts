import { defineConfig } from 'vitepress'

export default defineConfig({
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
          { text: 'Column Specification', link: '/getting-started/columns' },
        ]
      },
      {
        text: 'Examples',
        items: [
          { text: 'Nutanix to SQL Server', link: '/examples/nutanix-to-sqlserver' },
          { text: 'Rest API to iTop', link: '/examples/rest-api-to-itop' },
        ]
      },
      {
        text: 'Sources',
        items: [
          { text: 'SQL Server', link: '/sources/sqlserver' },
          { text: 'MySQL', link: '/sources/mysql' },
          { text: 'iTop', link: '/sources/itop' },
          { text: 'Postgres', link: '/sources/postgres' },
          { text: 'MongoDB', link: '/sources/mongodb' },
          { text: 'Rest API', link: '/sources/restapi' },
        ]
      },
      {
        text: 'Transformers',
        items: [
          { text: 'Using Transformers', link: '/transformers/using-transformers' },
          { text: 'Frames', link: '/transformers/frames' },
          { text: 'iTop', link: '/transformers/itop' },
          { text: 'Miscellaneous', link: '/transformers/misc' },
          { text: 'Regex', link: '/transformers/regex' },
          { text: 'Strings', link: '/transformers/strings' },
          { text: 'Structs', link: '/transformers/structs' },
          { text: 'Integers', link: '/transformers/int' },
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
