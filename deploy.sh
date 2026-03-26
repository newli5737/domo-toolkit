# ============================================
# DOMO Toolkit - VPS Deployment Guide
# ============================================

# 1. Clone & setup backend
cd /root
git clone https://github.com/newli5737/domo-toolkit.git
cd domo-toolkit/backend

# Tạo .venv và cài dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# hoặc nếu dùng uv:
# uv sync

# Copy .env lên VPS (chỉnh CORS_ORIGINS và DB)
# CORS_ORIGINS=https://domo.vanquyenhair.name.vn

# 2. Build frontend
cd /root/domo-toolkit/frontend
npm install
# Tạo .env cho production
echo "VITE_API_BASE=https://domo.vanquyenhair.name.vn" > .env
npm run build

# 3. PM2 — chạy backend
cd /root/domo-toolkit
pm2 start ecosystem.config.json
pm2 save
pm2 startup  # auto-start khi reboot

# 4. Nginx
sudo cp domo.vanquyenhair.name.vn.conf /etc/nginx/sites-available/domo.vanquyenhair.name.vn.conf
sudo ln -s /etc/nginx/sites-available/domo.vanquyenhair.name.vn.conf /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

# 5. Certbot SSL
sudo certbot --nginx -d domo.vanquyenhair.name.vn

# 6. Verify
pm2 status
curl https://domo.vanquyenhair.name.vn/api/auth/status
