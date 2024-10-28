// JavaScript to handle form submission and display loading spinner
const form = document.getElementById('uploadForm');
const spinner = document.getElementById('spinner');
const uploadButton = document.getElementById('uploadButton');

form.addEventListener('submit', function () {
    spinner.style.display = 'block'; // Show the spinner
    uploadButton.disabled = true;    // Disable the button
});
