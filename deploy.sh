#!/bin/bash
set -e

APP_DIR="/home/ubuntu/domo-toolkit"
DOMAIN="api-domo.dosutech.site"

# Clone or pull
cd /home/ubuntu
if [ ! -d "$APP_DIR" ]; then
    git clone https://github.com/newli5737/domo-toolkit.git
else
    cd $APP_DIR && git pull origin main
fi

cd $APP_DIR/backend

# Install uv if not present
if ! command -v uv &> /dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.local/bin/env
fi

uv sync

# Check .env
if [ ! -f .env ]; then
    echo "⚠️  .env not found! scp backend/.env to $APP_DIR/backend/.env first."
    exit 1
fi

# PostgreSQL
sudo systemctl enable postgresql
sudo systemctl start postgresql
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = 'domo-toolkit'" | \
    grep -q 1 || sudo -u postgres createdb "domo-toolkit"

# PM2
if ! command -v pm2 &> /dev/null; then npm install -g pm2; fi
cd $APP_DIR
pm2 delete domo-backend 2>/dev/null || true
pm2 start ecosystem.config.json
pm2 save
pm2 startup systemd -u root --hp /root 2>/dev/null || true

# Nginx
sudo cp $APP_DIR/$DOMAIN.conf /etc/nginx/sites-available/$DOMAIN.conf
[ ! -L /etc/nginx/sites-enabled/$DOMAIN.conf ] && \
    sudo ln -s /etc/nginx/sites-available/$DOMAIN.conf /etc/nginx/sites-enabled/$DOMAIN.conf
sudo nginx -t && sudo systemctl reload nginx

# SSL
if ! command -v certbot &> /dev/null; then
    sudo apt install -y certbot python3-certbot-nginx
fi
sudo certbot --nginx -d $DOMAIN --non-interactive --agree-tos -m newli5737@gmail.com --redirect

# Verify
pm2 status
curl -s https://$DOMAIN/api/health | python3 -m json.tool
