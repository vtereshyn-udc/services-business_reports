() => {
    const dropdown = document.querySelector('kat-dropdown#brand');
    if (!dropdown) return false;

    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const header = shadowRoot.querySelector('div[part="dropdown-header"]');
    if (!header) return false;

    return header.getAttribute('title');
}