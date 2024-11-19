const idSelect = document.getElementById('CpaPartId');
const downloadTemplate = document.getElementById('cpadownloadTemplateLink');
const prefillSkuCheckbox = document.getElementById('prefillSkuCheckbox');
const partType = document.getElementById('Cpatype');
const partCategory = document.getElementById('Cpacat');
const spinner = document.getElementById('spinner');
const uploadButton = document.getElementById('uploadButton'); // Assuming you have an uploadButton element

// Disable the download link initially
downloadTemplate.href = "";
downloadTemplate.classList.add("disabled");

function handleDownloadClick(event) {
    if (downloadTemplate.classList.contains("disabled")) {
        event.preventDefault();
        alert("Please select an ID from the dropdown before downloading the template.");
        return;
    }

    if (partType.value === '') {
        event.preventDefault();
        alert("Please select a Part Type from the dropdown before downloading the template.");
        return;
    }

    if (partCategory.value === '') {
        event.preventDefault();
        alert("Please select a Part Category from the dropdown before downloading the template.");
        return;
    }

    downloadCPATemplate(event);
}

async function downloadCPATemplate(event) {
    event.preventDefault(); // Prevent the default link behavior

    spinner.style.display = "block"; // Show the spinner
    uploadButton.disabled = true;  // Disable the button

    const url = downloadTemplate.href;

    try {
        if (url.includes('/upload_form_cust_part_attr')) {
            throw new Error("Something went wrong!");
        }

        const response = await fetch(url);

        if (response.ok) {
            const blob = await response.blob();
            const downloadUrl = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = downloadUrl;
            a.download = "cpa_" + new Date().toISOString().replace(/[-:T.Z]/g, '') + ".xlsx";
            document.body.appendChild(a);
            a.click();
            URL.revokeObjectURL(downloadUrl);
            a.remove();
        } else {
            console.error("Failed to download file:", response.statusText);
            alert('Something went wrong. Please try again.');
        }
    } catch (error) {
        console.error("Error fetching file:", error);
    } finally {
        spinner.style.display = "none"; // Hide the spinner when fetch completes
        uploadButton.disabled = false; // Re-enable the button
    }
}

// Attach the event listener once
downloadTemplate.addEventListener('click', handleDownloadClick);
