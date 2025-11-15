(el) => {
    const rect = el.getBoundingClientRect();
    return rect.width > 0 &&
           rect.height > 0 &&
           rect.top >= 0 &&
           rect.bottom <= window.innerHeight;
}