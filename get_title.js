() => {
    const dropdown = document.querySelector('#weekly-week');
    if (!dropdown) return false;

    const shadowRoot = dropdown.shadowRoot;
    if (!shadowRoot) return false;

    const firstChild = shadowRoot.querySelector('.select-header');
    if (!firstChild) return false;

    const secondChild = firstChild.querySelector('.header-row');
    if (!secondChild) return false;

    return firstChild.getAttribute('title');
}
