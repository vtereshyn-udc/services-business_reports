() => {
    const popover = document.querySelector('casino-tour-popover.hydrated');
    if (!popover) return false;

    const shadowRoot = popover.shadowRoot;
    if (!shadowRoot) return false;

    const dismissButton = shadowRoot.querySelector('casino-simple-button.dismiss-button.hydrated');
    if (!dismissButton) return false;

    dismissButton.click();
    return true;
}