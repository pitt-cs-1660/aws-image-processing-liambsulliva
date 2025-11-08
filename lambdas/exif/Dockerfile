FROM public.ecr.aws/lambda/python:3.13

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml ${LAMBDA_TASK_ROOT}/
COPY handler.py ${LAMBDA_TASK_ROOT}/

RUN uv pip install --system .

CMD ["handler.exif_handler"]
