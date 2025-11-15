() => {
    const dropdowns = document.querySelectorAll('kat-dropdown.daily-time-picker-kat-dropdown-normal.month-year-report-time-picker-month-select-style');
    if (dropdowns.length === 0) return false;

    const dropdown = dropdowns[0];
    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const header = shadowRoot.querySelector('div[part="dropdown-header"]');
    if (!header) return false;
    header.click();

    const options = shadowRoot.querySelectorAll('kat-option');
    if (options.length === 0) return false;

    options[options.length - 2].click();
    return true;
}