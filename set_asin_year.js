() => {
    const dropdowns = document.querySelectorAll('kat-dropdown#monthly-year');
    const dropdown = dropdowns[dropdowns.length - 1];
    if (!dropdown) return false;

    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const header = shadowRoot.querySelector('div[part="dropdown-header"]');
    if (!header) return false;
    header.click();

    const optionElements = shadowRoot.querySelectorAll('kat-option');
    const options = Array.from(optionElements).map(option => {
        const textElement = option.querySelector('span') || option;
        return textElement.textContent.trim();
    });

    if (options.length === 0) return false;

    const lastOption = options[0];
    for (const option of optionElements) {
        const text = (option.querySelector('span') || option).textContent.trim();
        if (text === lastOption) {
            option.click();
            return true;
        }
    }

    return false;
}