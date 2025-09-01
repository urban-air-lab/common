import io
import sys
import logging
import pytest
from ual.logging import get_logger


def _cleanup_logger(logger: logging.Logger):
    """Remove handlers and reset state to avoid cross-test pollution."""
    for h in list(logger.handlers):
        logger.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass
    logger.setLevel(logging.NOTSET)
    logger.propagate = True


def test_configures_stdout_streamhandler_and_formatter(monkeypatch):
    fake_stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", fake_stdout)

    name = "tests.get_logger.stdout_and_formatter"
    logger = get_logger(name)
    try:
        assert len(logger.handlers) == 1
        handler = logger.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        assert handler.stream is fake_stdout

        assert logger.level == logging.DEBUG
        assert logger.propagate is False

        fmt = "%(asctime)s %(levelname)s %(message)s"
        assert isinstance(handler.formatter, logging.Formatter)
        logger.info("hello")
        out = fake_stdout.getvalue()
        assert out.strip().endswith("INFO hello")
        assert "INFO hello" in out
    finally:
        _cleanup_logger(logger)


def test_idempotent_no_duplicate_handlers(monkeypatch):
    fake_stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdout", fake_stdout)

    name = "tests.get_logger.idempotent"
    logger1 = get_logger(name)
    try:
        assert len(logger1.handlers) == 1
        handler_id = id(logger1.handlers[0])

        logger2 = get_logger(name)
        assert logger2 is logger1
        assert len(logger1.handlers) == 1
        assert id(logger1.handlers[0]) == handler_id
    finally:
        _cleanup_logger(logger1)


def test_does_not_reconfigure_existing_logger(monkeypatch):
    name = "tests.get_logger.preconfigured"
    existing = logging.getLogger(name)
    stream = io.StringIO()
    pre_h = logging.StreamHandler(stream)
    existing.addHandler(pre_h)
    existing.setLevel(logging.WARNING)
    existing.propagate = True

    try:
        logger = get_logger(name)
        assert logger is existing
        assert len(logger.handlers) == 1
        assert logger.handlers[0] is pre_h
        assert logger.level == logging.WARNING
        assert logger.propagate is True

        logger.info("won’t show")
        logger.warning("will show")
        assert "will show" in stream.getvalue()
        assert "won’t show" not in stream.getvalue()
    finally:
        _cleanup_logger(existing)
