(year) => {
    const dropdowns = document.querySelectorAll('kat-dropdown[label="Event Year"]');
    const dropdown = dropdowns[dropdowns.length - 1];
    if (!dropdown) return false;

    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const header = shadowRoot.querySelector('div[part="dropdown-header"]');
    if (!header) return false;
    header.click();

    const options = shadowRoot.querySelectorAll('kat-option');
    for (const option of options) {
        const text = (option.querySelector('span') || option).textContent.trim();
        if (text === year) {
            option.click();
            return true;
        }
    }

    return false;
}