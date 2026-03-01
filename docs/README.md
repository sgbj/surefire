# Surefire docs

This folder contains the Surefire documentation site built with Astro + Starlight.

## Local development

Run these commands from this folder:

```bash
npm install
npm run dev
```

The local docs site starts at `http://localhost:4321`.

## Build and preview

```bash
npm run build
npm run preview
```

Production output is generated in `docs/dist`.

## Content structure

- Author docs pages in `src/content/docs`.
- Add static assets in `public`.
- Add authored images and media in `src/assets`.

## Main sections

- `getting-started/*`
- `concepts/*`
- `guides/*`
- `storage/*`
