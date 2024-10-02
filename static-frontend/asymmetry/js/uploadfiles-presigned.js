        // Upload files to s3 using presigned url and API gateway
        async function getPresignedUrl(objectName, customer, yearMonth) {
            const dateTime = new Date().toISOString().replace(/:/g, '-');
            const params = new URLSearchParams({
                object_name: objectName,
                customer: customer,
                year_month: yearMonth,
                date_time: dateTime
            });

            try {
                const response = await fetch(`https://7da9mwsko7.execute-api.us-east-2.amazonaws.com/upload?${params.toString()}`, {
                    method: 'PUT',
                    mode: 'cors',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({})
                });

                const responseText = await response.text();
                const responseJson = JSON.parse(responseText);

                if (responseJson.statusCode === 400 && responseJson.body === '"El documento ya existe"') {
                    throw new Error('DUPLICATE_DOCUMENT');
                }

                if (responseJson.statusCode !== 200) {
                    throw new Error(`HTTP error! status: ${responseJson.statusCode}`);
                }

                const innerJson = JSON.parse(responseJson.body);
                if (innerJson.url) {
                    return innerJson.url;
                } else {
                    throw new Error('Presigned URL not found in response');
                }
            } catch (error) {
                console.error('Error in getPresignedUrl:', error);
                throw error;
            }
        }

        async function uploadFile(event) {
            event.preventDefault();

            const customer = document.getElementById('customer').value.trim();
            const yearMonth = document.getElementById('year-month').value.trim();
            const fileInputElement = document.getElementById('file-input');
            const files = fileInputElement.files;
            const statusDiv = document.getElementById('upload-status');

            if (!customer || !yearMonth || files.length === 0) {
                alert('Please fill in all fields and select at least one file.');
                return;
            }

            const uploadButton = event.target;
            uploadButton.disabled = true;
            uploadButton.textContent = 'Uploading...';

            statusDiv.innerHTML = ''; // Clear previous status messages

            try {
                for (const file of files) {
                    const objectName = file.name;
                    const statusElement = document.createElement('p');
                    statusElement.textContent = `Uploading ${objectName}...`;
                    statusDiv.appendChild(statusElement);

                    try {
                        const presignedUrl = await getPresignedUrl(objectName, customer, yearMonth);

                        const response = await fetch(presignedUrl, {
                            method: 'PUT',
                            body: file,
                            headers: {
                                'Content-Type': file.type
                            }
                        });

                        if (!response.ok) {
                            throw new Error(`Failed to upload ${objectName}. Status: ${response.status}`);
                        }

                        statusElement.textContent = `${objectName} uploaded successfully!`;
                        statusElement.style.color = 'green';
                    } catch (error) {
                        if (error.message === 'DUPLICATE_DOCUMENT') {
                            statusElement.textContent = `${objectName} is a duplicate and was not uploaded.`;
                            statusElement.style.color = 'orange';
                        } else {
                            statusElement.textContent = `Error uploading ${objectName}: ${error.message}`;
                            statusElement.style.color = 'red';
                        }
                    }
                }

                alert('Upload process completed. Check the status for details on each file.');
            } catch (error) {
                console.error('Error during file upload:', error);
                alert(`Error: ${error.message}`);
            } finally {
                uploadButton.disabled = false;
                uploadButton.textContent = 'Upload and process file';
            }
        }

        // Add event listener when the DOM is fully loaded
        document.addEventListener('DOMContentLoaded', function() {
            const uploadButton = document.getElementById('upload-button');
            if (uploadButton) {
                uploadButton.addEventListener('click', uploadFile);
            } else {
                console.error('Upload button not found');
            }
        });