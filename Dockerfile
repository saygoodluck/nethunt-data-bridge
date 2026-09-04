FROM node:20-alpine

WORKDIR /app

# deps first, so code edits do not invalidate the install layer
COPY --chown=node:node package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --chown=node:node index.js telegram.js ./

# the SSH tunnel and the CRM calls are outbound only; nothing is written to disk
USER node

ENV NODE_ENV=production
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD wget --spider -q "http://127.0.0.1:${PORT:-3000}/" || exit 1

CMD ["node", "index.js"]
