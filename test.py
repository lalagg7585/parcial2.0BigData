import unittest
from unittest.mock import MagicMock, patch
from funcion_lambda import lambda_handler  # Asegúrate de importar tu función lambda_handler desde tu archivo lambda_function.py

class TestLambdaHandler(unittest.TestCase):
    
    @patch('funcion_lambda.boto3.client')
    def test_lambda_handler(self, mock_client):
        # Creamos un evento de prueba y un contexto de prueba
        event = {}
        context = MagicMock()
    
        # Llamamos a la función lambda_handler
        response = lambda_handler(event, context)
    
        # Verificamos que se haya llamado a boto3.client con los argumentos correctos
        mock_client.assert_called_once_with('emr', region_name='us-east-1')

        # Verificamos que la respuesta sea la esperada
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('Cluster', response['body'])

if __name__ == '__main__':
    unittest.main()
