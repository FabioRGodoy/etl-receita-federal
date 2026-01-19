#!/usr/bin/env node

import { testConnection, pool } from '../src/config/database.js';

/**
 * Teste rápido de conexão com o banco
 */

async function main() {
  console.log('\n' + '='.repeat(60));
  console.log('🔌 Teste de Conexão - PostgreSQL');
  console.log('='.repeat(60) + '\n');

  console.log('📋 Configurações:');
  console.log(`   Host: ${process.env.DB_HOST || '145.223.94.201'}`);
  console.log(`   Porta: ${process.env.DB_PORT || '5431'}`);
  console.log(`   Banco: ${process.env.DB_NAME || 'postgres'}`);
  console.log(`   Usuário: ${process.env.DB_USER || 'postgres'}`);
  console.log();

  try {
    console.log('🔍 Testando conexão...');
    const result = await testConnection();
    
    if (result.success) {
      console.log('✅ CONEXÃO OK!');
      console.log(`   Timestamp do servidor: ${result.timestamp}`);
      console.log();

      // Verificar versão do PostgreSQL
      console.log('🔍 Verificando versão do PostgreSQL...');
      const client = await pool.connect();
      let hasETLDb = false;
      
      try {
        const versionResult = await client.query('SELECT version()');
        console.log(`✅ ${versionResult.rows[0].version.split(',')[0]}`);
        console.log();

        // Listar bancos de dados disponíveis
        console.log('🔍 Listando bancos de dados...');
        const dbResult = await client.query(`
          SELECT datname FROM pg_database 
          WHERE datistemplate = false 
          ORDER BY datname
        `);
        console.log('   Bancos disponíveis:');
        dbResult.rows.forEach(row => {
          console.log(`   - ${row.datname}`);
        });
        console.log();

        // Verificar se existe o banco etl_receita_federal
        hasETLDb = dbResult.rows.some(row => row.datname === 'etl_receita_federal');
        
        if (hasETLDb) {
          console.log('✅ Banco "etl_receita_federal" encontrado!');
        } else {
          console.log('⚠️  Banco "etl_receita_federal" NÃO encontrado');
          console.log('   Execute: createdb etl_receita_federal');
        }

      } finally {
        client.release();
      }

      console.log();
      console.log('='.repeat(60));
      console.log('✅ TESTE DE CONEXÃO CONCLUÍDO!');
      console.log('='.repeat(60));
      console.log();
      
      if (!hasETLDb) {
        console.log('📝 Próximos passos:');
        console.log('   1. Criar banco: createdb -h 145.223.94.201 -p 5431 -U postgres etl_receita_federal');
        console.log('   2. Atualizar DB_NAME no .env para "etl_receita_federal"');
        console.log('   3. Criar schema: psql -h 145.223.94.201 -p 5431 -U postgres -d etl_receita_federal -f sql/schema.sql');
        console.log('   4. Rodar teste: node test-municipios-simples.js');
        console.log();
      } else {
        console.log('💡 Próximo passo: node test-municipios-simples.js');
        console.log();
      }

      process.exit(0);
    } else {
      throw new Error(result.error);
    }
  } catch (error) {
    console.error('❌ ERRO NA CONEXÃO!');
    console.error(`   ${error.message}`);
    console.error();
    console.error('🔧 Possíveis causas:');
    console.error('   - Firewall bloqueando conexão');
    console.error('   - Credenciais incorretas');
    console.error('   - PostgreSQL não está rodando');
    console.error('   - Host/porta incorretos');
    console.error();
    process.exit(1);
  }
}

main();
