// JavaScript to handle form submission and display loading spinner
const form = document.getElementById('uploadForm');
const spinner = document.getElementById('spinner');
const uploadButton = document.getElementById('uploadButton');
const download_cpa_template = document.getElementById('cpadownloadTemplateLink')

form.addEventListener('submit', function () {
    spinner.style.display = 'block'; // Show the spinner
    uploadButton.disabled = true;    // Disable the button
});


// Adding event listeners for close buttons
// Adding event listeners for close buttons


document.querySelectorAll('.flash-message').forEach(function(message) {
const closeBtn = document.createElement('span'); // Create close button
closeBtn.textContent = '×'; // Close button character
closeBtn.className = 'close-btn'; // Add a class for styling
closeBtn.addEventListener('click', function () {
    message.style.display = 'none'; // Hide the message
});
message.appendChild(closeBtn); // Append close button to message
});


if (download_cpa_template){
download_cpa_template.addEventListener("click", async function(event) {
    event.preventDefault(); // Prevent the default link behavior

    spinner.style.display = "block"; // Show the spinner
    uploadButton.disabled = true;  // Disable the button

    const url = this.href;

    try {
        // Fetch the file from the URL

        if( url.includes('/upload_form_cust_part_attr')){
            throw new Error("Something went wrong!");
        }

        const response = await fetch(url);

        if (response.ok) {
            // Convert the response to a Blob for file handling
            const blob = await response.blob();

            // Create a download link and trigger the download
            const downloadUrl = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = downloadUrl;
            a.download = "cpa_" + new Date().toISOString().replace(/[-:T.Z]/g, '') + ".csv"; // Define the download name
            document.body.appendChild(a);
            a.click();

            // Clean up by revoking the URL and removing the temporary element
            URL.revokeObjectURL(downloadUrl);
            a.remove();
        } else {
            console.error("Failed to download file:", response.statusText);
            alert('Something went wrong. Please try again')
        }
    } catch (error) {
        console.error("Error fetching file:", error);
    } finally {
        spinner.style.display = "none";  // Hide the spinner when fetch completes
        uploadButton.disabled = false; // Re-enable the button
    }
});
}
