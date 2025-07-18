const API_BASE_URL = 'http://localhost:8080';

async function putData() {
    const key = document.getElementById('putKey').value;
    const value = document.getElementById('putValue').value;
    const statusElement = document.getElementById('putStatus');

    if (!key || !value) {
        statusElement.textContent = 'Please enter both key and value.';
        statusElement.className = 'error';
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/put`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ key, value }),
        });
        const data = await response.json();
        if (response.ok) {
            statusElement.textContent = `Put successful: ${data.message}`;
            statusElement.className = 'success';
            document.getElementById('putKey').value = '';
            document.getElementById('putValue').value = '';
        } else {
            statusElement.textContent = `Error: ${data.error}`;
            statusElement.className = 'error';
        }
    } catch (error) {
        statusElement.textContent = `Network error: ${error.message}`;
        statusElement.className = 'error';
    }
}

async function getData() {
    const key = document.getElementById('getKey').value;
    const statusElement = document.getElementById('getStatus');

    if (!key) {
        statusElement.textContent = 'Please enter a key.';
        statusElement.className = 'error';
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/get/${key}`);
        const data = await response.json();
        if (response.ok) {
            if (data.found) {
                statusElement.textContent = `Key: ${data.Key}, Value: ${data.Value}`;
                statusElement.className = 'success';
            } else {
                statusElement.textContent = `Key '${data.Key}' not found.`;
                statusElement.className = 'error';
            }
        } else {
            statusElement.textContent = `Error: ${data.error}`;
            statusElement.className = 'error';
        }
    } catch (error) {
        statusElement.textContent = `Network error: ${error.message}`;
        statusElement.className = 'error';
    }
}

async function compactData() {
    const statusElement = document.getElementById('compactStatus');
    try {
        const response = await fetch(`${API_BASE_URL}/compact`, {
            method: 'POST',
        });
        const data = await response.json();
        if (response.ok) {
            statusElement.textContent = `Compaction: ${data.message}`;
            statusElement.className = 'success';
        } else {
            statusElement.textContent = `Error: ${data.error}`;
            statusElement.className = 'error';
        }
    } catch (error) {
        statusElement.textContent = `Network error: ${error.message}`;
        statusElement.className = 'error';
    }
}