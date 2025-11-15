() => {
    const dropdowns = document.querySelectorAll('kat-pagination');
    const secondDropdown = dropdowns[1];
    if (!secondDropdown) return false;

    const shadowRoot = secondDropdown.shadowRoot;
    if (!shadowRoot) return false;

    const header = shadowRoot.querySelector('span[part="pagination-nav-right"]');
    if (!header || header.style.display === 'none') return false;

    header.click();
    return true;
}