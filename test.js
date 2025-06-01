const isValidDomain = (domain) => {
    return /^[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+$/.test(domain);
};

console.log(isValidDomain('adnet.xxx'));
