() => {
    const icon = document.querySelector('kat-icon.inline-filter-chip-icon');
    if (!icon) return false;
    icon.click();

    setTimeout(() => {
        const radioButton = document.querySelector('kat-radiobutton[label="Custom date range"][name="lastUpdatedInline"]');
        if (!radioButton) return false;
        radioButton.click();
    }, 5000);
}