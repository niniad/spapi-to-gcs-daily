# SP-API to Google Cloud Storage Daily

This project serves as a Cloud Run service to fetch data from various Amazon Selling Partner API (SP-API) endpoints and upload it to Google Cloud Storage (GCS).

The service can run in two modes:
1.  **Production Mode**: Fetches data from all configured SP-API endpoints sequentially and in parallel.
2.  **Test Mode**: Fetches data from a single, specified SP-API endpoint.

## Project Structure

```
.
├── .idx/
│   └── dev.nix         # Nix configuration for the development environment
├── endpoints/
│   ├── __init__.py
│   ├── fba_inventory.py
│   ├── orders_api.py
│   └── ...             # Each file corresponds to one SP-API endpoint
├── utils/
│   ├── __init__.py
│   ├── auth.py         # Handles authentication with SP-API & GCP
│   └── ...
├── main.py             # Main entry point for the Cloud Run service
├── README.md           # This file
└── requirements.txt    # Python package dependencies
```

## Environment Setup (IDX)

This project is configured to work seamlessly with Google's IDX IDE.

1.  **GCP Secret Manager**:
    Ensure the following secrets are stored in your GCP project's Secret Manager:
    *   `SP_API_CLIENT_ID`: Your SP-API Client ID.
    *   `SP_API_CLIENT_SECRET`: Your SP-API Client Secret.
    *   `SP_API_REFRESH_TOKEN`: Your SP-API Refresh Token.

2.  **Environment Variables**:
    The `.idx/dev.nix` file is pre-configured to read these secret names. It automatically sets the necessary environment variables (`SP_API_CLIENT_ID_SECRET_ID`, etc.) for the application to use.

3.  **Workspace Loading**:
    When the IDX workspace loads, it uses the `dev.nix` file to:
    *   Install the correct Python version.
    *   Install all Python packages listed in `requirements.txt`.
    *   Set the environment variables.

    **No manual `pip install` is required.**

## Local Testing (Using the IDX Terminal)

You can test the entire process or individual endpoints directly from the IDX terminal. The `main.py` file is configured to run as a local web server using the `functions-framework`.

1.  **Start the Local Server**:
    Open a terminal in IDX and run the following command:
    ```bash
    functions-framework --target=main
    ```
    This will start a local server, typically on `http://localhost:8080`.

2.  **Execute All Endpoints (Production Mode)**:
    In a new terminal, use `curl` to send a POST request to the local server.
    ```bash
    curl -X POST http://localhost:8080
    ```

3.  **Execute a Single Endpoint (Test Mode)**:
    To test a specific endpoint, add the `endpoint` parameter to the URL in your `curl` command. The endpoint name must match one of the keys in the `ENDPOINT_MAP` in `main.py`.

    Example for `orders_api`:
    ```bash
    curl -X POST "http://localhost:8080?endpoint=orders_api"
    ```

    Example for `sales_and_traffic`:
    ```bash
    curl -X POST "http://localhost:8080?endpoint=sales_and_traffic"
    ```

## Deployment

This service is intended for deployment on Google Cloud Run. Deployment is handled via GitHub integration, which automatically builds and deploys the service when changes are pushed to the main branch. The `requirements.txt` file ensures all dependencies are installed in the production environment.
