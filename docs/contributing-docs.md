# Building this documentation

This site is built from the `vgi-rpc-c` repository using MkDocs Material, following
the C++, Go, and Python implementations. Guides live in `docs/`; navigation,
theme settings, and strict link checking live in `mkdocs.yml`.

## Preview locally

Use a separate documentation environment. These Python packages are development
tools and are not dependencies of the C runtime.

```sh
python3 -m venv .venv-docs
.venv-docs/bin/python -m pip install -r requirements-docs.txt
.venv-docs/bin/mkdocs serve
```

Open `http://127.0.0.1:8000`. Changes to documentation are rebuilt during preview.
To produce the static site and validate links:

```sh
.venv-docs/bin/mkdocs build --strict
git diff --check
```

The output is written to `site/`, which is ignored by Git. Broken documentation
links, missing anchors, omitted navigation pages, and missing source includes
fail the strict build.

## Source includes

The [calculator example](examples.md) includes `examples/calculator.c` and
`examples/client.c`. The [API reference](api/index.md) includes the public headers
in `include/vgi_rpc/`. Edit those C sources to change displayed declarations;
do not copy a second version into Markdown. Keep explanations and ownership rules
in the guides consistent with implementation changes.

## Publication

The `Docs` GitHub Actions workflow builds pull requests and uploads the static
site as the `documentation` artifact. Pushes to `main` build and deploy to the
Cloudflare Pages project `vgi-rpc-c`, serving
[vgi-rpc-c.query.farm](https://vgi-rpc-c.query.farm/). It also runs when included
headers or examples change, and supports manual dispatch from `main`.

Deployment uses the repository's `CLOUDFLARE_API_TOKEN` and
`CLOUDFLARE_ACCOUNT_ID` Actions secrets. The Pages project and its custom domain
are provisioned separately; the workflow updates the existing project.

The site and first-party source use the
[Query Farm Source-Available License, Version 1.0](https://github.com/Query-farm/vgi-rpc-c/blob/main/LICENSE).
Dependencies retain their own licenses.
