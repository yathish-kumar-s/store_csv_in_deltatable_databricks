
const idSelect = document.getElementById('CpaPartId');
const downloadTemplate =  document.getElementById('cpadownloadTemplateLink')
const prefillSkuCheckbox = document.getElementById('prefillSkuCheckbox');

downloadTemplate.href = "";  // Disable the download link initially
downloadTemplate.classList.add("disabled");  // Add the disabled class

downloadTemplate.addEventListener('click', function(event) {
if (downloadTemplate.classList.contains("disabled")) {
    event.preventDefault();  // Prevent the default action
    alert("Please select an ID from the dropdown before downloading the template.");
}
});