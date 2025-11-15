() => {
    const dropdown = document.querySelector('kat-dropdown#brand');
    if (!dropdown) return false;

    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const optionElements = shadowRoot.querySelectorAll('kat-option');
    const options = Array.from(optionElements).map(option => {
        const textElement = option.querySelector('span') || option;
        return textElement.textContent.trim();
    });

    return options;
}