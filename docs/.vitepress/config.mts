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
        text: 'Getting Started',
        items: [
          { text: 'Markdown Examples', link: '/getting-started' },
          { text: 'Runtime API Examples', link: '/api-reference' }
        ]
      },
      {
        text: 'API Reference',
        items: [
          { text: 'Markdown Examples', link: '/getting-started' },
          { text: 'Runtime API Examples', link: '/api-reference' }
        ]
      },
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/vuejs/vitepress' }
    ]
  }
})
