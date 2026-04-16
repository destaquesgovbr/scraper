"""
Integration test configuration and fixtures.

Provides skip-count enforcement to prevent silent test failures when network is unavailable.
"""

import pytest


def pytest_sessionfinish(session, exitstatus):
    """
    Warn if too many integration tests were skipped.

    Integration tests make real HTTP requests and can skip due to network issues.
    If >50% of integration tests skip, it likely indicates a systemic problem
    (network down, firewall blocking, gov.br maintenance) rather than individual
    test issues. Fail the test run to prevent false positives.
    """
    if session.config.getoption("--collect-only"):
        return

    integration_skipped = 0
    integration_passed = 0
    integration_failed = 0
    integration_total = 0

    for item in session.items:
        if item.get_closest_marker("integration"):
            integration_total += 1

            # Skips can occur in setup phase (fixture calls pytest.skip) or call phase
            setup_skipped = hasattr(item, "rep_setup") and item.rep_setup.skipped
            call_skipped = hasattr(item, "rep_call") and item.rep_call.skipped

            if setup_skipped or call_skipped:
                integration_skipped += 1
            elif hasattr(item, "rep_call") and item.rep_call.passed:
                integration_passed += 1
            elif hasattr(item, "rep_call") and item.rep_call.failed:
                integration_failed += 1

    # If we haven't run yet, check during collection (for --collect-only compatibility)
    if integration_total == 0:
        return

    if integration_total > 0:
        skip_percentage = (integration_skipped / integration_total) * 100

        # Print summary
        print("\n" + "=" * 70)
        print("INTEGRATION TEST SUMMARY")
        print("=" * 70)
        print(f"Total:   {integration_total}")
        print(f"Passed:  {integration_passed}")
        print(f"Failed:  {integration_failed}")
        print(f"Skipped: {integration_skipped} ({skip_percentage:.1f}%)")
        print("=" * 70)

        # Fail if too many skips
        if skip_percentage > 50:
            pytest.exit(
                f"\nFAIL: {skip_percentage:.1f}% of integration tests were skipped "
                f"({integration_skipped}/{integration_total}).\n"
                f"This likely indicates network unavailability or gov.br maintenance.\n"
                f"Integration tests require real HTTP access to validate behavior.",
                returncode=1,
            )


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    Store test results on items so pytest_sessionfinish can access them.
    """
    outcome = yield
    rep = outcome.get_result()

    # Store report by phase
    setattr(item, f"rep_{rep.when}", rep)
