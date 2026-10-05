"""
Utility functions for managing logs, streaming messages, etc.
"""
def log(msg):
    print(f"[producer] {msg}", flush=True)