# Marketplace Feature Implementation Guide

## 📋 Summary of Changes

This document outlines all the changes made to implement the **Message-Based Marketplace** feature in your messaging app.

---

## 🗄️ Database Changes

### New Tables Created:

1. **`marketplace_items`** - Stores products for sale
   - Links to `messages` table (one-to-one)
   - Tracks seller, chat, price, images, status
   - Supports price negotiations

2. **`purchases`** - Tracks transactions
   - Links buyer, seller, and item
   - Stores PayPal payment IDs and status
   - Tracks purchase lifecycle

3. **`seller_ratings`** - Reputation system
   - Buyers rate sellers after purchases
   - 1-5 star rating + optional comment
   - One rating per purchase

### Updated Relationships:

- **Users** now have relationships:
  - `items_sold` - Items they're selling
  - `purchases_made` - Items they bought
  - `purchases_received` - Items they sold
  - `ratings_received` - Ratings they got
  - `ratings_given` - Ratings they gave

- **Chats** now have:
  - `marketplace_items` - All items for sale in that chat

- **Messages** now have:
  - `marketplace_item` - Optional link to marketplace listing

---

## 🆕 New API Endpoints

### Marketplace Management:

1. **`POST /messages/{message_id}/sell`**
   - Convert a message into a marketplace listing
   - Requires: title, price, description, images
   - Returns: MarketplaceItem

2. **`GET /marketplace`**
   - List all marketplace items with filters:
     - `chat_id` - Filter by chat
     - `seller_id` - Filter by seller
     - `status` - Filter by status (active, sold, cancelled)
     - `min_price` / `max_price` - Price range
     - `search` - Search in title/description
   - Supports pagination

3. **`GET /marketplace/{item_id}`**
   - Get specific item with seller info

4. **`PUT /marketplace/{item_id}`**
   - Update item (price, description, status, etc.)
   - Supports price negotiations via `current_price`

### Purchase Flow:

5. **`POST /marketplace/{item_id}/purchase`**
   - Create a purchase (initiate PayPal checkout)
   - Validates item is available
   - Creates pending purchase record
   - Returns purchase with PayPal order ID (TODO)

6. **`POST /marketplace/purchases/{purchase_id}/complete`**
   - Complete purchase after PayPal payment
   - Updates item status to "sold"
   - Records completion timestamp

### Reviews:

7. **`POST /marketplace/purchases/{purchase_id}/rate`**
   - Rate seller after purchase
   - 1-5 stars + optional comment
   - One rating per purchase

### Analytics:

8. **`GET /marketplace/stats`**
   - Get marketplace statistics:
     - Total items, active items, sold items
     - Total revenue
     - Top sellers ranking
   - Can filter by chat_id or seller_id

---

## 📦 New Dependencies

Added to `requirements.txt`:
- `paypalrestsdk==1.13.3` - PayPal SDK for payment processing

---

## 🔧 Implementation Details

### Database Schema Fields:

**MarketplaceItem:**
- `message_id` - Links to original message (unique)
- `seller_id` - User selling the item
- `chat_id` - Chat where item is listed
- `title` - Product name (max 200 chars)
- `description` - Product description
- `price` - Original price (Decimal, 2 decimals)
- `current_price` - Negotiated price (nullable)
- `currency` - Currency code (default: USD)
- `image_urls` - JSON array of image URLs
- `status` - active, sold, cancelled, pending
- `is_negotiable` - Whether price can be negotiated

**Purchase:**
- `item_id` - Marketplace item being purchased
- `buyer_id` - User buying
- `seller_id` - User selling
- `amount` - Purchase amount
- `paypal_order_id` - PayPal order ID
- `paypal_payment_id` - PayPal payment ID
- `paypal_status` - Payment status
- `status` - pending, completed, cancelled, refunded

**SellerRating:**
- `seller_id` - User being rated
- `buyer_id` - User giving rating
- `purchase_id` - Purchase this rating is for (unique)
- `rating` - 1-5 stars
- `comment` - Optional review text

---

## 🚀 Next Steps (TODO)

### PayPal Integration:
1. **Add PayPal SDK setup** in a separate module
2. **Create PayPal order** in `create_purchase` endpoint
3. **Verify PayPal payment** in `complete_purchase` endpoint
4. **Add PayPal webhook** for payment status updates
5. **Environment variables** needed:
   - `PAYPAL_CLIENT_ID`
   - `PAYPAL_CLIENT_SECRET`
   - `PAYPAL_MODE` (sandbox/production)

### Additional Features to Consider:
- Image upload endpoint (store images, return URLs)
- Price negotiation messages in chat
- Purchase cancellation/refund flow
- Seller dashboard (items sold, revenue, ratings)
- Buyer purchase history
- Chat notifications for new marketplace items

---

## 📝 Example Usage Flow

1. **User sends a message** in a group chat:
   ```
   POST /messages
   {
     "content": "Selling my old laptop for $500",
     "chat_id": 1,
     "sender_id": 1
   }
   ```

2. **Convert message to listing**:
   ```
   POST /messages/123/sell
   {
     "message_id": 123,
     "chat_id": 1,
     "title": "Laptop for Sale",
     "price": 500.00,
     "description": "Dell XPS 13, excellent condition",
     "image_urls": ["https://example.com/laptop.jpg"]
   }
   ```

3. **Buyer browses marketplace**:
   ```
   GET /marketplace?chat_id=1&status=active&min_price=400&max_price=600
   ```

4. **Buyer purchases item**:
   ```
   POST /marketplace/1/purchase?buyer_id=2
   ```

5. **Complete purchase after PayPal payment**:
   ```
   POST /marketplace/purchases/1/complete?paypal_payment_id=PAY123456
   ```

6. **Buyer rates seller**:
   ```
   POST /marketplace/purchases/1/rate
   {
     "purchase_id": 1,
     "rating": 5,
     "comment": "Great seller, fast shipping!"
   }
   ```

---

## ✅ Testing Checklist

- [ ] Create marketplace item from message
- [ ] List marketplace items with filters
- [ ] Update marketplace item (price, status)
- [ ] Create purchase
- [ ] Complete purchase
- [ ] Rate seller
- [ ] View marketplace stats
- [ ] Test pagination
- [ ] Test search functionality
- [ ] Verify database constraints (unique message_id, etc.)

---

## 🎯 Key Features Implemented

✅ Convert messages to marketplace listings  
✅ Search and filter marketplace items  
✅ Price negotiations  
✅ Purchase flow with PayPal integration points  
✅ Seller rating system  
✅ Marketplace analytics  
✅ Multi-image support  
✅ Status tracking (active, sold, pending, cancelled)  

---

**Note:** PayPal integration is stubbed with TODO comments. You'll need to implement the actual PayPal API calls using the PayPal SDK.

