# Email Security Backend - API Documentation

## Authentication

### JWT Tokens

After OAuth login, you receive:

- `jwt_token` (access token, valid 15 minutes)
- `refresh_token` (valid 7 days)

### Auth Rule for Frontend

- Protected HTTP endpoints use only the `Authorization: Bearer <jwt_token>` header.
- WebSocket endpoints use the JWT in the query string, for example `?token=<jwt_token>`.
- Do not send both a query token and an Authorization header for the same HTTP request.
- In Swagger UI, use the **Authorize** button (top-right) to set the bearer token once.

## Email Endpoints

### 1. GET /auth/google/login

**Purpose:**  
Initiate Google OAuth login flow.

**Auth Required:** No

**HTTP Method:** GET

**URL:** `/auth/google/login`

**Query Parameters:** None

**Response:**

- Redirects to Google consent screen.
- After user approves, Google redirects to `/auth/google/callback`.

**Example Request:**

```
GET http://127.0.0.1:8000/auth/google/login
```

**Scopes Requested:**

- `openid email profile`
- `https://www.googleapis.com/auth/gmail.readonly` (read emails)
- `https://www.googleapis.com/auth/gmail.send` (send emails)

---

### 2. GET /auth/google/callback

**Purpose:**  
Google OAuth callback. Exchanges authorization code for tokens. Returns JWT.

**Auth Required:** No (called by Google)

**HTTP Method:** GET

**URL:** `/auth/google/callback?code=...`

**Query Parameters:**

- `code` (required): Authorization code from Google

**Response JSON:**

```json
{
  "jwt_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expires_in": 900,
  "user": {
    "user_id": "42",
    "google_id": "110170705258730366018",
    "name": "John Doe",
    "email": "john@gmail.com",
    "photo_url": "https://lh3.googleusercontent.com/...",
    "provider": "google"
  }
}
```

**Status Codes:**

- `200` OK - successful login
- `502` Bad Gateway - Google API error

**Example Request:**

```
GET http://127.0.0.1:8000/auth/google/callback?code=4/0AX4XfWgZq...
```

---

## Important Note:

front end call the login endpoint and the endpoint callback is called internal not by the front end and he get the information about the login

---

### 3. POST /auth/logout-and-reauth

**Purpose:**  
Clear Google tokens and force re-authorization (useful when fixing scope issues).

**Auth Required:** Yes (JWT)

**HTTP Method:** POST

**URL:** `/auth/logout-and-reauth`

**Request Headers:**

```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Request Body:** Empty or `{}`

**Response JSON:**

```json
{
  "message": "Tokens cleared. Please go to /auth/google/login to re-authorize with the new permissions.",
  "redirect_url": "/auth/google/login"
}
```

**Status Codes:**

- `200` OK
- `401` Unauthorized - missing/invalid JWT

**Example Request:**

```bash
curl -X POST http://127.0.0.1:8000/auth/logout-and-reauth \
  -H "Authorization: Bearer eyJhbGc..." \
  -H "Content-Type: application/json"
```

---

### 4. POST /labels

**Purpose:**  
Create a label for the authenticated user.

**Auth Required:** Yes (JWT)

**HTTP Method:** POST

**URL:** `/labels`

**Request Body (JSON):**

```json
{
  "name": "Important",
  "color": "#ff9900"
}
```

**Body Parameters:**

- `name` (required, string): Label name
- `color` (optional, string): Label color value

**Response JSON (Success):**

```json
{
  "label_id": 12,
  "name": "Important",
  "color": "#ff9900",
  "created_at": "2026-06-24T10:00:00+00:00"
}
```

**Status Codes:**

- `200` OK
- `400` Bad Request - missing or empty label name
- `401` Unauthorized

---

### 5. POST /label-rules

**Purpose:**  
Add a user to a specific label by creating a row in the `label_rules` table and backfilling matching emails into `email_labels`.

**Auth Required:** Yes (JWT)

**HTTP Method:** POST

**URL:** `/label-rules`

**Request Body (JSON):**

```json
{
  "label_id": 12,
  "from_user_id": 7
}
```

**Body Parameters:**

- `label_id` (required, integer): The authenticated user's label id
- `from_user_id` (required, integer): The user id to add to the label

**Behavior:**

- Adds a `label_rules` row for the authenticated user.
- Finds all existing emails received by the current user from `from_user_id` and creates matching rows in `email_labels`.
- Reusing the same rule is idempotent and only backfills missing `email_labels` rows.

**Response JSON (Success):**

```json
{
  "rule_id": 3,
  "label_id": 12,
  "from_user_id": 7,
  "created": true,
  "tagged_emails_count": 42
}
```

**Status Codes:**

- `200` OK
- `400` Bad Request - invalid ids
- `401` Unauthorized
- `404` Not Found - label or user not found

---

### 6. POST /emails/send

**Purpose:**  
Send an email via Gmail to one or more recipients. Stores email in backend.

**Auth Required:** Yes (JWT)

**HTTP Method:** POST

**URL:** `/emails/send`

**Request Headers:**

```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Request Body (JSON):**

```json
{
  "recipients": ["alice@example.com", "bob@example.com"],
  "subject": "Test Email",
  "body": "Hello team, this is a test message."
}
```

**Body Parameters:**

- `recipients` (required, array of strings): Email addresses of recipients
- `subject` (optional, string): Email subject line
- `body` (optional, string): Email body/message content

**Response JSON (Success):**

```json
{
  "email_id": 15,
  "gmail_message_id": "187c1b5e87a10b23",
  "status": "sent"
}
```

**Status Codes:**

- `200` OK - email sent successfully
- `400` Bad Request - invalid recipients or malformed body
- `401` Unauthorized - missing/invalid JWT
- `502` Bad Gateway - Gmail API error or insufficient permissions

**Error Responses:**

1. Missing JWT:

```json
{
  "detail": "Missing JWT token"
}
```

2. Invalid recipients:

```json
{
  "detail": "recipients must be a non-empty list of email addresses"
}
```

3. Insufficient Gmail scopes (before fix):

```json
{
  "detail": "Failed to send email: {...} | RESOLUTION: Visit https://myaccount.google.com/permissions, revoke this app, then re-login via /auth/google/login"
}
```

---

### 7. GET /emails

**Purpose:**  
List the authenticated user's emails with optional filters for delivery status, label, and sender.

**Auth Required:** Yes (JWT)

**HTTP Method:** GET

**URL:** `/emails`

**Query Parameters:**

- `status` (optional, string): `draft`, `sent`, or `all` (default: `all`)
- `label_id` (optional, integer): Filter emails tagged with this label
- `from_user_id` (optional, integer): Filter emails sent by this user

**Response JSON (Success):**

```json
{
  "emails": [
    {
      "email_id": 1,
      "subject": "Meeting Notes"
    }
  ]
}
```

---

### 8. PATCH /emails/{email_id}/read

**Purpose:**  
Mark an email as read.

**Auth Required:** Yes (JWT)

**HTTP Method:** PATCH

**URL:** `/emails/{email_id}/read`

**Path Parameters:**

- `email_id` (required, integer): The database ID of the email

**Request Headers:**

```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Request Body:** Empty or `{}`

**Response JSON:**

```json
{
  "email_id": 5,
  "is_read": true
}
```

**Status Codes:**

- `200` OK
- `401` Unauthorized - missing/invalid JWT
- `404` Not Found - email not found or not accessible by user

---

### 9. PATCH /emails/{email_id}/trash

**Purpose:**  
Set or clear the trash flag on an email.

**Auth Required:** Yes (JWT)

**HTTP Method:** PATCH

**URL:** `/emails/{email_id}/trash`

**Path Parameters:**

- `email_id` (required, integer): The database ID of the email

**Query Parameters:**

- `value` (optional, boolean): `true` to mark as trash, `false` to untrash (default: `true`)

**Request Headers:**

```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Request Body:** Empty or `{}`

**Response JSON (Trash):**

```json
{
  "email_id": 5,
  "is_trash": true
}
```

**Response JSON (Untrash):**

```json
{
  "email_id": 5,
  "is_trash": false
}
```

**Status Codes:**

- `200` OK
- `401` Unauthorized
- `404` Not Found

---

### 10. PATCH /emails/{email_id}/star

**Purpose:**  
Set or clear the starred flag on an email.

**Auth Required:** Yes (JWT)

**HTTP Method:** PATCH

**URL:** `/emails/{email_id}/star`

**Path Parameters:**

- `email_id` (required, integer): The database ID of the email

**Query Parameters:**

- `value` (optional, boolean): `true` to star, `false` to unstar (default: `true`)

**Request Headers:**

```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Request Body:** Empty or `{}`

**Response JSON (Star):**

```json
{
  "email_id": 5,
  "is_starred": true
}
```

**Response JSON (Unstar):**

```json
{
  "email_id": 5,
  "is_starred": false
}
```

**Status Codes:**

- `200` OK
- `401` Unauthorized
- `404` Not Found

---

### 11. PATCH /emails/{email_id}/draft_edit

**Purpose:**  
Edit an existing draft email owned by the authenticated user.

**Auth Required:** Yes (JWT)

**HTTP Method:** PATCH

**URL:** `/emails/{email_id}/draft_edit`

**Path Parameters:**

- `email_id` (required, integer): The database ID of the draft email

**Request Body:**

Use `application/json` for text-only edits, or `multipart/form-data` when adding attachment files.

**JSON Example:**

```json
{
  "subject": "Updated draft subject",
  "body": "Updated draft body",
  "recipients": ["alice@example.com", "bob@example.com"],
  "delete_attachment_ids": [12, 15]
}
```

**Body Parameters:**

- `subject` (optional, string): New draft subject
- `body` (optional, string): New draft body
- `recipients` (optional, array of strings): New recipient list
- `delete_attachment_ids` (optional, array of integers): Attachment ids to remove from the draft
- `files` (optional, uploaded files): Attachment files to add when using multipart/form-data

**Multipart Form Fields:**

- `subject` (optional, string)
- `body` (optional, string)
- `recipients` (optional, repeated string field or JSON array string)
- `delete_attachment_ids` (optional, repeated integer field or JSON array string)
- `files` (optional, one or more uploaded files)

**Response JSON (Success):**

```json
{
  "email_id": 5,
  "status": "draft",
  "delivery_status": "draft",
  "subject": "Updated draft subject",
  "body": "Updated draft body",
  "recipients": ["alice@example.com", "bob@example.com"],
  "attachments": []
}
```

**Status Codes:**

- `200` OK
- `400` Bad Request - only draft emails can be edited or recipients are invalid
- `401` Unauthorized
- `404` Not Found

---

### 12. DELETE /emails/{email_id}

**Purpose:**  
Delete an email from the database and remove it from Gmail when it exists there.

**Auth Required:** Yes (JWT)

**HTTP Method:** DELETE

**URL:** `/emails/{email_id}`

**Path Parameters:**

- `email_id` (required, integer): The database ID of the email

**Request Headers:**

```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Response JSON (Success):**

```json
{
  "email_id": 5,
  "deleted": true,
  "gmail_deleted": true
}
```

**Notes:**

- If the email has a `gmail_message_id`, the backend deletes the Gmail message first and then removes the local database row plus related records.
- Gmail deletion requires `gmail.modify` authorization. If the user has not reauthorized since this scope was added, the API returns a `502` with a re-login hint.

**Status Codes:**

- `200` OK
- `401` Unauthorized
- `404` Not Found
- `502` Bad Gateway - Gmail API error or insufficient permissions

---

## Error Codes

### HTTP Status Codes

| Status | Meaning                                          |
| ------ | ------------------------------------------------ |
| 200    | OK - Request succeeded                           |
| 400    | Bad Request - Invalid parameters                 |
| 401    | Unauthorized - Missing or invalid JWT            |
| 404    | Not Found - Resource doesn't exist               |
| 502    | Bad Gateway - Upstream service error (Gmail API) |

### Error Response Format

All errors follow this JSON structure:

```json
{
  "detail": "Human-readable error message"
}
```

### Common Errors

**401 - Missing JWT Token**

```json
{
  "detail": "Missing JWT token"
}
```

**401 - Invalid/Expired JWT**

```json
{
  "detail": "Invalid JWT token"
}
```

**401 - JWT Expired**

```json
{
  "detail": "JWT token expired"
}
```

**404 - Email Not Found**

```json
{
  "detail": "Email not found"
}
```

**400 - Invalid Recipients**

```json
{
  "detail": "recipients must be a non-empty list of email addresses"
}
```

---

## WebSocket Endpoints

### 1. WebSocket /ws/emails

**Purpose:**  
Receive initial email batch and real-time new email notifications.

**Auth Required:** Yes (JWT via query parameter)

**WebSocket URL:** `ws://127.0.0.1:8000/ws/emails?token=<jwt_token>`

**Server Messages:**

1. **initial_emails** (sent on connect):

```json
{
  "type": "initial_emails",
  "emails": [
    {
      "email_id": 1,
      "gmail_message_id": "187c1b5e87a10b23",
      "thread_id": "18790e9e85e4a68e",
      "subject": "Meeting Notes",
      "snippet": "Thanks for joining the call. Here are the notes...",
      "body": "Thanks for joining the call. Here are the notes from today's discussion...",
      "category": "Primary",
      "date": "2026-05-05T10:30:00+00:00",
      "status": "PENDING",
      "risk_score": 0.0,
      "final_verdict": null,
      "urls_status": "PENDING",
      "body_status": "PENDING",
      "headers_status": "PENDING",
      "attachments_status": "PENDING",
      "is_read": false,
      "is_hooked": false,
      "is_trash": false,
      "is_starred": false,
      "sender": {
        "user_id": 7,
        "email": "sender@example.com",
        "name": "Sender Name",
        "photo_url": null,
        "provider": "external"
      }
    }
  ]
}
```

2. **email_received** (new incoming email during session):

```json
{
  "type": "email_received",
  "email": {
    "email_id": 5,
    "gmail_message_id": "187d2c6f98b11c34",
    "thread_id": "18790e9e85e4a69f",
    "subject": "Project Update",
    "snippet": "Quick update on the project status...",
    "body": "Quick update on the project status...",
    "category": "Primary",
    "date": "2026-05-05T14:22:00+00:00",
    "status": "PENDING",
    "risk_score": 0.0,
    "final_verdict": null,
    "urls_status": "PENDING",
    "body_status": "PENDING",
    "headers_status": "PENDING",
    "attachments_status": "PENDING",
    "is_read": false,
    "is_hooked": false,
    "is_trash": false,
    "is_starred": false,
    "sender": {
      "user_id": 7,
      "email": "sender@example.com",
      "name": "Sender Name",
      "photo_url": null,
      "provider": "external"
    }
  }
}
```

**Status Codes:**

- `1008` - Policy Violation (invalid/missing JWT)

---

### 2. WebSocket /ws/updates

**Purpose:**  
Receive analysis progress updates (partial results and completion).

**Auth Required:** Yes (JWT via query parameter)

**WebSocket URL:** `ws://127.0.0.1:8000/ws/updates?token=<jwt_token>`

**Server Messages:**

1. **partial_update** (URLs case):

```json
{
  "type": "partial_update",
  "user_id": 42,
  "email_id": 5,
  "field": "urls",
  "status": "DONE",
  "urls": [
    {
      "url": "https://example.com",
      "verdict": "clean",
      "reasons": ["dummy-url-check"],
      "status": "DONE"
    }
  ]
}
```

2. **partial_update** (Headers case):

```json
{
  "type": "partial_update",
  "user_id": 42,
  "email_id": 5,
  "field": "headers",
  "status": "DONE",
  "headers": {
    "verdict": "clean",
    "reasons": ["dummy-headers-check"],
    "status": "DONE"
  }
}
```

3. **partial_update** (Body case):

```json
{
  "type": "partial_update",
  "user_id": 42,
  "email_id": 5,
  "field": "body",
  "status": "DONE",
  "body": {
    "verdict": "clean",
    "confidence": 0.01,
    "status": "DONE"
  }
}
```

4. **partial_update** (Attachments case):

```json
{
  "type": "partial_update",
  "user_id": 42,
  "email_id": 5,
  "field": "attachments",
  "status": "DONE",
  "attachments": [
    {
      "file_name": "invoice.pdf",
      "file_type": "application/pdf",
      "file_size": 245621,
      "hash_sha256": "a1b2c3d4...",
      "status": "DONE",
      "verdict": "suspicious",
      "reasons": []
    }
  ]
}
```

5. **analysis_complete** (all parts analyzed):

```json
{
  "type": "analysis_complete",
  "user_id": 42,
  "email_id": 5,
  "status": "ANALYZED",
  "risk_score": 5.0,
  "final_verdict": "SAFE",
  "is_hooked": false,
  "is_trash": false,
  "is_starred": false,
  "category": "Primary",
  "urls": [
    {
      "verdict": "clean",
      "reasons": ["dummy-url-check"],
      "status": "DONE"
    }
  ],
  "body": {
    "verdict": "clean",
    "confidence": 0.01,
    "status": "DONE"
  },
  "headers": {
    "verdict": "clean",
    "reasons": ["dummy-headers-check"],
    "status": "DONE"
  },
  "attachments": []
}
```

**Field Values:**

- `field`: `urls` | `body` | `headers` | `attachments`
- `status`: `PENDING` | `PROCESSING` | `DONE` | `FAILED`
- `final_verdict`: `SAFE` | `PHISHING`
- `risk_score`: float (0.0 - 100.0)
