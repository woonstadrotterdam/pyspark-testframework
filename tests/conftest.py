def pytest_sessionfinish(session, exitstatus):
    if exitstatus == 5:  # Exit code 5 means no tests were collected
        session.exitstatus = 0  # Change the exit code to 0 (success)
