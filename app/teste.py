import subprocess
import re

# Caminho do arquivo assinado
file_path = 'app/pgcc.txt.p7s'  # Substitua pelo caminho correto do arquivo


# Função para extrair informações do certificado usando OpenSSL
def extract_certificate_info_openssl(file_path):
    # Comando OpenSSL para extrair o certificado do arquivo .p7s
    extract_cert_cmd = ["openssl", "pkcs7", "-inform", "DER", "-in", file_path, "-print_certs"]

    # Executar o comando e capturar a saída
    result = subprocess.run(extract_cert_cmd, capture_output=True, text=True)
    cert_data = result.stdout

    if not cert_data:
        raise ValueError("Certificado não encontrado ou falha ao extrair certificado.")

    # Comando OpenSSL para exibir as informações do certificado
    show_cert_cmd = ["openssl", "x509", "-noout", "-text"]

    # Executar o comando e capturar a saída, passando cert_data como entrada
    result = subprocess.run(show_cert_cmd, input=cert_data, capture_output=True, text=True)
    cert_info = result.stdout

    return cert_info


# Função para extrair o CNPJ das informações do certificado
def extract_cnpj(cert_info):
    # Ajustar a regex para buscar CNPJ após "LTDA:"
    cnpj_pattern = re.compile(r'LTDA:(\d{14})')
    match = cnpj_pattern.search(cert_info)

    if match:
        return match.group(1)
    else:
        return None


# Função para extrair o conteúdo do arquivo sem o certificado
def extract_content_without_cert(file_path):
    # Comando OpenSSL para extrair o conteúdo do arquivo .p7s
    extract_content_cmd = ["openssl", "smime", "-verify", "-inform", "DER", "-in", file_path, "-noverify", "-outform",
                           "PEM"]

    # Executar o comando e capturar a saída
    result = subprocess.run(extract_content_cmd, capture_output=True, text=True)
    content = result.stdout

    if not content:
        raise ValueError("Conteúdo não encontrado ou falha ao extrair conteúdo.")

    return content


def run():
    # Extrair e imprimir as informações do certificado
    certificate_info = extract_certificate_info_openssl(file_path)
    cnpj = extract_cnpj(certificate_info)

    print("Informações do Certificado:")
    print(certificate_info)

    if cnpj:
        print(f"CNPJ: {cnpj}")
    else:
        print("CNPJ não encontrado no certificado.")

    # Extrair e imprimir o conteúdo sem o certificado
    content_without_cert = extract_content_without_cert(file_path)

    print("Conteúdo do Arquivo (sem o certificado):")
    print(content_without_cert[:1000])  # Primeiros 1000 caracteres do conteúdo
