() => {
    const dropdowns = document.querySelectorAll('#weekly-week');
    const dropdown = dropdowns[dropdowns.length - 1];
    if (!dropdown) return false;

    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const header = shadowRoot.querySelector('div[part="dropdown-header"]');
    if (!header) return false;
    header.click();

    const options = shadowRoot.querySelectorAll('kat-option');
    if (options.length === 0) return false;

    options[0].click();
    return true;
}