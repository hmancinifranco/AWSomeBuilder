function populateClienteDropdown() {
	fetch('https://your-api-gateway-endpoint.com/clientes')
		.then(response => response.json())
		.then(data => {
			const clienteSelect = document.getElementById('cliente');
			data.forEach(cliente => {
				const option = document.createElement('option');
				option.value = cliente.id;
				option.textContent = cliente.nombre;
				clienteSelect.appendChild(option);
			});
		})
		.catch(error => console.error('Error fetching clientes:', error));
}

document.addEventListener('DOMContentLoaded', populateClienteDropdown);
