(date) => {
    const datePickers = document.querySelectorAll('kat-date-picker[autocomplete="off"]');
    const datePickerEnd = datePickers[1];

    const inputEnd = datePickerEnd.shadowRoot.querySelector('kat-input[part="date-picker-input"]');

    if (inputEnd) {
        inputEnd.value = date;
        const inputEvent = new Event('input', { bubbles: true });
        const changeEvent = new Event('change', { bubbles: true });
        inputEnd.dispatchEvent(inputEvent);
        inputEnd.dispatchEvent(changeEvent);

    }
}