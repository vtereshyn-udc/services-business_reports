(date) => {
    const datePickers = document.querySelectorAll('kat-date-picker[autocomplete="off"]');
    const datePickerStart = datePickers[0];

    const inputStart = datePickerStart.shadowRoot.querySelector('kat-input[part="date-picker-input"]');

    if (inputStart) {
        inputStart.value = date;
        const inputEvent = new Event('input', { bubbles: true });
        const changeEvent = new Event('change', { bubbles: true });
        inputStart.dispatchEvent(inputEvent);
        inputStart.dispatchEvent(changeEvent);

    }
}