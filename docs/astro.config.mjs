// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
	site: 'https://batary.dev',
	base: '/surefire',
	integrations: [
		starlight({
			title: 'Surefire',
			description: 'Distributed job scheduling for .NET',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/sgbj/surefire' }],
			customCss: ['./src/styles/custom.css'],
			expressiveCode: {
				styleOverrides: { borderRadius: '0.5rem' },
			},
			sidebar: [
				{
					label: 'Getting started',
					items: [
						{ label: 'Getting started', slug: 'getting-started' },
					],
				},
				{
					label: 'Concepts',
					items: [
						{ label: 'Architecture', slug: 'concepts/architecture' },
					],
				},
				{
					label: 'Guides',
					items: [
						{ label: 'Dashboard', slug: 'guides/dashboard' },
					],
				},
			],
		}),
	],
});
