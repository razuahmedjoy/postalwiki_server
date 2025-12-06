module.exports = {
  apps: [{
    name: 'admin-backend',
    script: './server.js',
    instances: 1, // or 'max' for cluster mode
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'development',
      PORT: 3000
    },
    env_production: {
      NODE_ENV: 'production',
      PORT: process.env.PORT || 3000
    },
    // Logging
    log_file: './logs/combined.log',
    out_file: './logs/out.log',
    error_file: './logs/error.log',
    log_date_format: 'YYYY-MM-DD HH:mm Z',
    
    // Advanced settings
    min_uptime: '10s',
    max_restarts: 10,
    restart_delay: 1000
  }]
};