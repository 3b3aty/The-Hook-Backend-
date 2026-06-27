# Email Security Backend - API Documentation

## Table of Contents

- Authentication
- OAuth flow
- HTTP endpoints (labels, emails, auth)
- WebSocket endpoints
- Errors and status codes
- Examples

## Authentication

### JWT Tokens (current behavior)

After a successful OAuth login the server issues two JWTs:

- `jwt_token` (access token): valid for 7 days (used for API requests and websocket auth)
- `refresh_token`: valid for 7 days (stored server-side and can be used to manage sessions)

Notes:

- The API currently uses a single long-lived token strategy (both access and refresh are 7 days). In production you may prefer short-lived access tokens (minutes) and long-lived refresh tokens.
- The token payload includes `user_id`, `email`, `type` (`access` or `refresh`), and `exp` (expiry).

### How to send tokens

- HTTP: include the access token in the `Authorization` header:

```
Authorization: Bearer <jwt_token>
```

- WebSocket: include the token as a query parameter: `/ws/emails?token=<jwt_token>`.
- Do not provide the token both in a query string and in the `Authorization` header for the same request.

### Expires / TTL values

- `expires_in` (seconds) returned by the server when issuing tokens: `604800` (7 days).

## OAuth Flow

1. Client requests `GET /auth/google/login` — server redirects to Google consent screen with scopes `openid email profile` and Gmail scopes.
2. After user consent, Google redirects to `/auth/google/callback?code=...`.
3. Server exchanges the `code` for Google tokens, retrieves the user's profile, creates or updates the local `User` record, issues JWTs and returns them to the client.

Important integration note:

- For single-page apps the front-end should open `GET /auth/google/login` to start the flow; the server handles the callback and returns the resulting tokens to the client (see the example response below).

## HTTP Endpoints

The API exposes standard REST endpoints. All endpoints below that require authentication expect a valid `jwt_token` as the `Authorization` header.

### 1) GET /auth/google/login

Purpose: Redirect user agent to Google OAuth consent.

Auth: No

Response: HTTP 302 redirect to Google consent URL.

### 2) GET /auth/google/callback

Purpose: OAuth callback — exchanges code for Google tokens and issues JWTs for the app.

Auth: No (called by Google)

Query parameters:

- `code` (required): authorization code from Google

Response JSON on success:

```json
{
  "jwt_token": "eyJ...",
  "refresh_token": "eyJ...",
  "expires_in": 604800,
  "user": {
    "user_id": 42,
    "google_id": "110170705258730366018",
    "name": "John Doe",
    "email": "john@gmail.com",
    "photo_url": "https://lh3.googleusercontent.com/...",
    "provider": "google"
  }
}
```

Status Codes:

- `200` OK — successful login
- `502` Bad Gateway — Google API error

### 3) POST /auth/logout-and-reauth

Purpose: Clear stored Google tokens for the user and force re-authorization.

Auth: Yes

Request headers:

```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

Response JSON:

```json
{
  "message": "Tokens cleared. Please go to /auth/google/login to re-authorize with the new permissions.",
  "redirect_url": "/auth/google/login"
}
```

Status codes: `200`, `401` if token missing or invalid.

### 4) POST /labels

Purpose: Create a user label.

Auth: Yes

Request JSON:

```json
{
  "name": "Important",
  "color": "#ff9900"
}
```

Response JSON (201 or 200):

```json
{
  "label_id": 12,
  "name": "Important",
  "color": "#ff9900",
  "created_at": "2026-06-24T10:00:00+00:00"
}
```

Errors: `400` bad input, `401` unauthorized.

### 5) POST /label-rules

Purpose: Add a rule that maps incoming emails from a given sender to a label and retroactively applies it to stored emails.

Auth: Yes

Request JSON:

```json
{
  "label_id": 12,
  "from_user_id": 7
}
```

Response JSON:

```json
{
  "rule_id": 3,
  "label_id": 12,
  "from_user_id": 7,
  "created": true
}
```

Errors: `400`, `401`, `404`.

### 6) POST /emails/send

Purpose: Send an email via Gmail on behalf of the authenticated user and persist a copy locally.

Auth: Yes

Request JSON:

```json
{
  "recipients": ["alice@example.com", "bob@example.com"],
  "subject": "Test Email",
  "body": "Hello team, this is a test message."
}
```

Response JSON on success:

```json
{
  "email_id": 15,
  "gmail_message_id": "187c1b5e87a10b23",
  "status": "sent"
}
```

Errors:

- `400` invalid recipients
- `401` missing/invalid JWT
- `502` Gmail API error or insufficient scopes

### 7) GET /emails

Purpose: List authenticated user's emails with optional filters.

Auth: Yes

Query params:

- `status` (`draft`|`sent`|`all`) — default `all`
- `label_id` (int) — filter by label
- `from_user_id` (int) — filter by sender

Response JSON: paginated `emails` array.

### 8) PATCH /emails/{email_id}/read

Purpose: Mark an email read.

Auth: Yes

Response JSON:

```json
{
  "email_id": 5,
  "is_read": true
}
```

Errors: `401`, `404`.

### 9) PATCH /emails/{email_id}/trash

Purpose: Set or clear trash flag on an email. Query `value=true|false`.

Auth: Yes

Response JSON: `is_trash` flag.

### 10) PATCH /emails/{email_id}/star

Purpose: Set or clear starred flag. Query `value=true|false`.

Auth: Yes

Response JSON: `is_starred` flag.

### 11) PATCH /emails/{email_id}/draft_edit

Purpose: Edit an existing draft. Use `multipart/form-data` when uploading attachments.

Auth: Yes

Request fields: `subject`, `body`, `recipients`, `delete_attachment_ids`, `files`.

### 12) DELETE /emails/{email_id}

Purpose: Delete an email and remove it from Gmail if present.

Auth: Yes

Response JSON:

```json
{
  "email_id": 5,
  "deleted": true,
  "gmail_deleted": true
}
```

Errors: `401`, `404`, `502`.

## WebSocket Endpoints

WebSocket connections require a valid `jwt_token` provided as a query parameter (`?token=<jwt_token>`).

### 1) /ws/emails

Purpose: On connect, send an `initial_emails` message with user emails, then push `email_received` messages for new incoming mail.

Example connect URL:

```
ws://127.0.0.1:8000/ws/emails?token=<jwt_token>
```

Messages:

- `initial_emails` — full initial batch
- `email_received` — single new email

### 2) /ws/updates

Purpose: Receive incremental analysis updates for email processing (URLs, headers, body, attachments).

Messages include `partial_update` and `analysis_complete` events.

## Error Codes

Common HTTP status codes used by the API:

- `200` OK
- `201` Created
- `400` Bad Request
- `401` Unauthorized — missing/invalid JWT
- `404` Not Found
- `502` Bad Gateway — upstream (Google/Gmail) or integration error

Error response format:

```json
{
  "detail": "Human-readable error message"
}
```

Common error messages:

- `Missing JWT token` — no token provided
- `Invalid JWT token` — signature invalid
- `JWT token expired` — token `exp` passed

## Examples

1. Call protected endpoint with curl:

```bash
curl -H "Authorization: Bearer $JWT" http://127.0.0.1:8000/emails
```

2. Connect to emails websocket (Node example):

```js
const ws = new WebSocket("ws://127.0.0.1:8000/ws/emails?token=" + JWT);
ws.onmessage = (m) => console.log(JSON.parse(m.data));
```

## Notes & Recommendations

- The current token strategy is simple; consider shortening `jwt_token` lifetime and introducing a refresh endpoint.
- Ensure `JWT_SECRET` is set in production and rotated periodically.
- Keep `GOOGLE_CLIENT_SECRET` out of source control and restrict redirect URIs in Google Console.

If you want, I can also generate OpenAPI-compatible examples or add a `curl` script directory with quick integration tests.

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
Add a user to a specific label by creating a row in the `label_rules` table and applying the rule to existing emails from that sender.

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

- Creates a new `label_rules` row.
- Inserts `email_labels` rows for any existing emails received from `from_user_id` so they immediately belong to the label.
- Any new incoming emails from `from_user_id` are also automatically recorded in `email_labels` for this label when they arrive.

```json
{
  "rule_id": 3,
  "label_id": 12,
  "from_user_id": 7,
  "created": true
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

**Behavior:**

- Marks the local email as read.
- If the email has a `gmail_message_id`, it also removes the `UNREAD` label in Gmail.

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

**Behavior:**

- Marks the local email as trash/untrash.
- If the email has a `gmail_message_id`, it also adds/removes the `TRASH` label in Gmail.

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

**Behavior:**

- Marks the local email as starred/unstarred.
- If the email has a `gmail_message_id`, it also adds/removes the `STARRED` label in Gmail.

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
