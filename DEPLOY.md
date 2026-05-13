# Stage + Deploy Setup (GitHub Actions + PM2 on Plesk)

Auto deploy is configured in `.github/workflows/deploy.yml`:

- push to `develop` => runs STAGE checks only (no deploy)
- push to `main` => deploys to PROD target

## 1) GitHub Secrets

Set these repository secrets.

PROD:
- `PROD_SSH_HOST`
- `PROD_SSH_PORT` (optional, default `22`)
- `PROD_SSH_USER`
- `PROD_SSH_PRIVATE_KEY`
- `PROD_SERVER_APP_DIR`

## 2) Plesk mapping (required)

For PROD deploy, use these values from Plesk:

- `*_SSH_HOST`: server IP or hostname
- `*_SSH_PORT`: usually `22`
- `*_SSH_USER`: subscription system user in Plesk (with SSH enabled)
- `*_SERVER_APP_DIR`: app folder for that subscription
- `*_SSH_PRIVATE_KEY`: private key matching the public key authorized for that system user

## 3) First-time server setup (once per target server)

```bash
sudo apt update
sudo apt install -y git
```

If Node is via `nodenv`, ensure Node 22 or 24 is installed for the deploy user.
If PM2 is missing:

```bash
npm i -g pm2
```

Optional first run test:

```bash
mkdir -p <SERVER_APP_DIR>
cd <SERVER_APP_DIR>
git clone <YOUR_REPO_URL> .
npm ci --omit=dev
pm2 start ecosystem.config.cjs --env production
pm2 save
```

## 4) Deploy flow

- push `develop` => GitHub Action runs stage only (no deploy)
- push `main` => GitHub Action deploys to PROD
