(picker, {startDate, endDate}) => {
    const datePickerStart = picker.shadowRoot.querySelector('kat-date-picker.start');
    const datePickerEnd = picker.shadowRoot.querySelector('kat-date-picker.end');

    const inputStart = datePickerStart.shadowRoot.querySelector('kat-input[part="date-picker-input"]');
    const inputEnd = datePickerEnd.shadowRoot.querySelector('kat-input[part="date-picker-input"]');

    if (inputStart) {
        inputStart.value = startDate;
        const inputEvent = new Event('input', { bubbles: true });
        const changeEvent = new Event('change', { bubbles: true });
        inputStart.dispatchEvent(inputEvent);
        inputStart.dispatchEvent(changeEvent);

    }
    if (inputEnd) {
        inputEnd.value = endDate;
        const inputEvent = new Event('input', { bubbles: true });
        const changeEvent = new Event('change', { bubbles: true });
        inputEnd.dispatchEvent(inputEvent);
        inputEnd.dispatchEvent(changeEvent);

    }
}