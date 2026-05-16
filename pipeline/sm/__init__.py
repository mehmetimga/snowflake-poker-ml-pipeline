"""SageMaker entrypoints — thin wrappers around pipeline.* training functions.

These are designed to be invoked by SageMaker Training / Processing jobs. Each
entrypoint reads SageMaker-injected env vars (channel paths, model dir) and
delegates to the existing training functions in `pipeline/<module>/train.py`.
"""
