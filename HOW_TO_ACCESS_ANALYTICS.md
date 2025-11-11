# How to Access Analytics Tables

## 📊 Option 1: Metabase (Recommended - Visual Interface)

**URL:** http://localhost:3000

### Setup Steps:
1. Open http://localhost:3000 in your browser
2. Complete initial setup (if first time):
   - Create admin account
   - Connect to analytics database:
     - **Database Type:** PostgreSQL
     - **Host:** `analyticsdb`
     - **Port:** `5432`
     - **Database:** `analyticsdb`
     - **Username:** `analyticsuser`
     - **Password:** `analyticspassword`
3. Browse tables and create dashboards!

### Available Tables (after ETL runs):
- `user_statistics` - User analytics
- `chat_statistics` - Chat analytics  
- `daily_message_stats` - Daily metrics
- `hourly_message_stats` - Hourly distribution
- `weekday_message_stats` - Weekday distribution (Mon..Sun)
- `message_type_summary` - Message types breakdown
- `marketplace_statistics` - Marketplace rollup (items, avg price, etc.)
- `top_sellers` - Top sellers by items/revenue proxy
- `category_statistics` - Per-category items, statuses, average price
- `seller_statistics` - Per-seller listings, sold, avg price, value totals
- `chat_marketplace_stats` - Marketplace activity by chat
- `daily_marketplace_stats` - Daily marketplace listings/sales
- `seller_category_stats` - Sellers per category

---

## 💻 Option 2: Direct Database Access (psql)

### Connect to Analytics DB:
```bash
docker compose exec analyticsdb psql -U analyticsuser -d analyticsdb
```

### Useful Queries:
```sql
-- List all tables
\dt

-- View all analytics tables
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' 
ORDER BY table_name;

-- Query user statistics
SELECT * FROM user_statistics LIMIT 10;

-- Query chat statistics
SELECT * FROM chat_statistics ORDER BY total_messages DESC LIMIT 10;

-- Query daily stats
SELECT * FROM daily_message_stats ORDER BY date DESC LIMIT 30;

-- Query marketplace stats (after ETL runs)
SELECT * FROM marketplace_statistics;
SELECT * FROM top_sellers ORDER BY items_sold DESC;
SELECT * FROM category_statistics ORDER BY sold_items DESC LIMIT 20;
SELECT * FROM seller_statistics ORDER BY sold_items DESC LIMIT 20;
SELECT * FROM chat_marketplace_stats ORDER BY total_items DESC LIMIT 20;
SELECT * FROM daily_marketplace_stats ORDER BY date DESC LIMIT 30;
SELECT * FROM seller_category_stats ORDER BY sellers_count DESC;
```

---

## 🌐 Option 3: PGAdmin (If you want a database GUI)

You can use any PostgreSQL client:
- **Host:** `localhost`
- **Port:** `5434` (analyticsdb)
- **Database:** `analyticsdb`
- **Username:** `analyticsuser`
- **Password:** `analyticspassword`

---

## 📝 Option 4: Query via Docker Command

```bash
# Query any table directly
docker compose exec analyticsdb psql -U analyticsuser -d analyticsdb \
  -c "SELECT * FROM user_statistics LIMIT 5;"

# Count records in each table
docker compose exec analyticsdb psql -U analyticsuser -d analyticsdb \
  -c "SELECT 'user_statistics' as table, COUNT(*) FROM user_statistics
      UNION ALL
      SELECT 'chat_statistics', COUNT(*) FROM chat_statistics
      UNION ALL  
      SELECT 'daily_message_stats', COUNT(*) FROM daily_message_stats;"
```

---

## 🎯 Quick Access Commands

```bash
# View all tables
docker compose exec analyticsdb psql -U analyticsuser -d analyticsdb -c "\dt"

# Interactive psql session
docker compose exec -it analyticsdb psql -U analyticsuser -d analyticsdb

# Query specific table
docker compose exec analyticsdb psql -U analyticsuser -d analyticsdb \
  -c "SELECT * FROM user_statistics ORDER BY total_messages_sent DESC LIMIT 10;"
```

---

## 📊 Current Analytics Tables Available:

1. ✅ `user_statistics` - User metrics
2. ✅ `chat_statistics` - Chat metrics
3. ✅ `daily_message_stats` - Daily activity
4. ✅ `hourly_message_stats` - Hour patterns
5. ✅ `weekday_message_stats` - Weekday patterns
6. ✅ `message_type_summary` - Message types
7. ✅ `marketplace_statistics` - Marketplace rollup
8. ✅ `top_sellers` - Top sellers
9. ✅ `category_statistics` - By category
10. ✅ `seller_statistics` - By seller
11. ✅ `chat_marketplace_stats` - By chat (marketplace)
12. ✅ `daily_marketplace_stats` - Daily marketplace
13. ✅ `seller_category_stats` - Sellers per category

---

## 🚀 Best Practice: Use Metabase

Metabase is the easiest way to:
- ✅ Browse all tables visually
- ✅ Create queries with a GUI
- ✅ Build dashboards and charts
- ✅ Share reports with your team
- ✅ No SQL knowledge required!

**Access it at:** http://localhost:3000

