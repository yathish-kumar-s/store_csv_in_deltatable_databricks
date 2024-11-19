// JavaScript to handle form submission and display loading spinner
const form = document.getElementById('uploadForm');
const upload_spinner = document.getElementById('spinner');
const uploadButton = document.getElementById('uploadButton');
const download_cpa_template = document.getElementById('cpadownloadTemplateLink')

form.addEventListener('submit', function () {
    upload_spinner.style.display = 'block'; // Show the spinner
    uploadButton.disabled = true;    // Disable the button
});


document.querySelectorAll('.flash-message').forEach(function(message) {
const closeBtn = document.createElement('span'); // Create close button
closeBtn.textContent = '×'; // Close button character
closeBtn.className = 'close-btn'; // Add a class for styling
closeBtn.addEventListener('click', function () {
    message.style.display = 'none'; // Hide the message
});
message.appendChild(closeBtn); // Append close button to message
});

