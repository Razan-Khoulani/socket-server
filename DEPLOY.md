# Deploy Setup (GitHub Actions + PM2)

Auto deploy is configured in `.github/workflows/deploy.yml`:

- push to `develop` => deploys to DEV target
- push to `main` => deploys to PROD target

## 1) GitHub Secrets

Set these repository secrets.

DEV:
- `DEV_SSH_HOST`
- `DEV_SSH_PORT` (optional, default `22`)
- `DEV_SSH_USER`
- `DEV_SSH_PRIVATE_KEY`
- `DEV_SERVER_APP_DIR`

PROD:
- `PROD_SSH_HOST`
- `PROD_SSH_PORT` (optional, default `22`)
- `PROD_SSH_USER`
- `PROD_SSH_PRIVATE_KEY`
- `PROD_SERVER_APP_DIR`

## 2) First-time server setup (once per target server)

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

## 3) Deploy flow

- push `develop` => GitHub Action deploys to DEV
- push `main` => GitHub Action deploys to PROD
