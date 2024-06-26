import subprocess
import re
import time

# Caminho do arquivo assinado
file_path = "C:/Users/DEV LENOVO/Desktop/DESIF/validacao_desif/pgcc.txt.p7s"  # Substitua pelo caminho correto do arquivo


# Função para extrair todas as informações necessárias em uma única leitura do arquivo
def extract_all_info(file_path):
    # Comando OpenSSL para extrair o certificado e o conteúdo do arquivo .p7s
    extract_all_cmd = ["openssl", "smime", "-pk7out", "-inform", "DER", "-in", file_path]

    # Executar o comando e capturar a saída
    result = subprocess.run(extract_all_cmd, capture_output=True, text=True)
    pkcs7_data = result.stdout

    if not pkcs7_data:
        raise ValueError("Falha ao extrair informações do arquivo.")

    # Comando OpenSSL para extrair o certificado do PKCS7 data
    extract_cert_cmd = ["openssl", "pkcs7", "-inform", "PEM", "-print_certs"]

    # Executar o comando e capturar a saída, passando pkcs7_data como entrada
    result = subprocess.run(extract_cert_cmd, input=pkcs7_data, capture_output=True, text=True)
    cert_data = result.stdout

    if not cert_data:
        raise ValueError("Certificado não encontrado ou falha ao extrair certificado.")

    # Comando OpenSSL para exibir as informações do certificado
    show_cert_cmd = ["openssl", "x509", "-noout", "-text"]

    # Executar o comando e capturar a saída, passando cert_data como entrada
    result = subprocess.run(show_cert_cmd, input=cert_data, capture_output=True, text=True)
    cert_info = result.stdout
    print(cert_info)
    # Comando OpenSSL para extrair o conteúdo real do arquivo .p7s
    extract_content_cmd = ["openssl", "smime", "-verify", "-inform", "DER", "-in", file_path, "-noverify", "-outform",
                           "PEM"]

    # Executar o comando e capturar a saída
    result = subprocess.run(extract_content_cmd, capture_output=True)
    content = result.stdout

    if not content:
        raise ValueError("Conteúdo não encontrado ou falha ao extrair conteúdo.")

    content = content.decode('utf-8', errors='ignore')

    return cert_info, content


# Função para extrair o CNPJ das informações do certificado
def extract_cnpj(cert_info):
    # Ajustar a regex para buscar CNPJ após "LTDA:"
    cnpj_pattern = re.compile(r'LTDA:(\d{14})')
    match = cnpj_pattern.search(cert_info)

    if match:
        return match.group(1)
    else:
        return None



# Extrair todas as informações
time_start = time.time()
certificate_info, content_without_cert = extract_all_info(file_path)
time_end = time.time()
print("Tempo total para extrair as informações: ", time_end - time_start)

# Extrair e imprimir as informações do certificado
print("Informações do Certificado:")
print(certificate_info)
cnpj = extract_cnpj(certificate_info)

if cnpj:
    print(f"CNPJ: {cnpj}")
else:
    print("CNPJ não encontrado no certificado.")

# Extrair e imprimir o conteúdo sem o certificado
print("Conteúdo do Arquivo (sem o certificado):")
print(content_without_cert[:1000])  # Primeiros 1000 caracteres do conteúdo


