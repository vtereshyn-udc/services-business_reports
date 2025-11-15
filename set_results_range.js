(value) => {
    const dropdowns = document.querySelectorAll(`kat-dropdown[value="${value}"]`);
    if (dropdowns.length === 0) return false;

    const dropdown = dropdowns[0];
    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const header = shadowRoot.querySelector('div[part="dropdown-header"]');
    if (!header) return false;
    header.click();

    const options = shadowRoot.querySelectorAll('kat-option');
    if (options.length === 0) return false;

    options[options.length - 1].click();
    return true;
}