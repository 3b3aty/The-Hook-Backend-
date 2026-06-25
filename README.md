# Email Security Backend

Backend service for Gmail-integrated email ingestion, analysis, and realtime updates. It provides Google OAuth login, email fetch/send APIs, asynchronous analysis via Celery, and WebSocket push notifications via Redis Pub/Sub.

## Components

- **FastAPI app**: API endpoints, OAuth flow, Gmail integration, and WebSocket gateways in [main.py](main.py).
- **Database layer**: SQLAlchemy engine/session setup in [database.py](database.py) and ORM models in [models.py](models.py).
- **Task queue**: Celery app and routing in [celery_app.py](celery_app.py).
- **Email analysis tasks**: URL, body, headers, and attachment processing in [tasks/](tasks/).
- **Analysis service**: External analysis API client stubs in [services/analysis_service.py](services/analysis_service.py).
- **Message queue producer**: Celery task enqueue in [message_queue/producer.py](message_queue/producer.py).
- **Realtime updates**: Redis Pub/Sub publisher in [redis_pubsub.py](redis_pubsub.py) and WebSocket manager in [websocket/manager.py](websocket/manager.py).
- **API docs**: Endpoint reference in [API_doc.md](API_doc.md).

## Requirements

- Python 3.10+ recommended
- RabbitMQ (Celery broker)
- Redis (Pub/Sub)

## Installation

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with the variables below.

## Environment Variables

```ini
# OAuth / Google
GOOGLE_CLIENT_ID=your-google-client-id
GOOGLE_CLIENT_SECRET=your-google-client-secret
GOOGLE_REDIRECT_URI=http://localhost:8000/auth/google/callback
GCP_PUBSUB_SUBSCRIPTION=your-GCP_PUBSUB_SUBSCRIPTION
GCP_PUBSUB_TOPIC=your-GCP_PUBSUB_SUBSCRIPTION

# JWT
JWT_SECRET=change-this

# Database
DATABASE_URL=sqlite:///./test.db

# Celery / RabbitMQ
CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//
CELERY_RESULT_BACKEND=cache+memory://

# Redis
REDIS_URL=redis://localhost:6379/0
REDIS_WS_CHANNEL=ws_updates
```

## Running

1. **Open Command Prompt in the project folder**

2. **Activate the virtual environment:**

   ```bash
   venv\Scripts\activate
   ```

3. **Start Celery workers** (in one terminal):

   ```bash
   celery -A celery_app.celery_app worker -Q url_queue,body_queue,headers_queue,attachments_queue --loglevel=info --pool=solo --concurrency=1 --without-mingle --without-gossip
   ```

4. **Set up ngrok** (in a separate command prompt):
   - Navigate to the folder where ngrok is stored
   - Run:
     ```bash
     ngrok http 8000
     ```

5. **Run the project** (in the project terminal):
   ```bash
   uvicorn main:app --reload
   ```

## Notes

- OAuth login starts at `GET /auth/google/login`, and Google redirects to `GOOGLE_REDIRECT_URI`.
- WebSocket endpoints expect JWT in the query string, for example: `/ws/emails?token=...`.
- Analysis tasks currently use stubbed API calls in [services/analysis_service.py](services/analysis_service.py). Replace them with your deployed analysis services if needed.

## API Reference

See [API_doc.md](API_doc.md) for endpoint details and examples.
