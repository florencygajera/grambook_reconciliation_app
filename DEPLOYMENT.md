# Deployment Guide

This project is set up to deploy on Vercel using the Flask app in `grambook_app/app.py`.

## What was added

- `api/index.py` - Vercel entrypoint that imports the Flask app
- `vercel.json` - routes all traffic to the Flask function
- root `requirements.txt` - Python dependencies for Vercel builds

## Required environment variables

Set this in the Vercel dashboard under Project Settings > Environment Variables:

- `GRAMBOOK_SECRET_KEY`

Use a long random value and keep it the same across deployments so session-based CSRF continues to work.

## Deploy steps

1. Push the repository to GitHub, GitLab, or Bitbucket.
2. Import the repo into Vercel.
3. Confirm Vercel detects the root `requirements.txt`.
4. Add `GRAMBOOK_SECRET_KEY` to the environment variables.
5. Deploy the project.

## Notes

- The app serves the frontend from `grambook_app/static/index.html`.
- Runtime cache files in `grambook_app/.grambook_cache/` are excluded from the Vercel bundle.
- If you change the secret key later, existing sessions will be invalidated.

## Local run

```bash
pip install -r requirements.txt
python grambook_app/app.py
```

