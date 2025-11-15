() => {
    const dropdowns = document.querySelectorAll('kat-dropdown[data-testid="selection-box-dropdown"]');
    const dropdown = dropdowns[0];
    if (!dropdown) return false;

    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const header = shadowRoot.querySelector('div[part="dropdown-header"]');
    if (!header) return false;
    header.click();

    const options = shadowRoot.querySelectorAll('kat-option');
    for (const option of options) {
        const text = (option.querySelector('span') || option).textContent.trim();
        if (text === 'Custom') {
            option.click();
            return true;
        }
    }
    return false;
}