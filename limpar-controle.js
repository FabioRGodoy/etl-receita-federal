import { pool } from './src/config/database.js';

async function limparControle() {
  const client = await pool.connect();
  
  try {
    console.log('🧹 Limpando tabelas de controle...');
    
    // Limpar arquivos de controle
    const filesResult = await client.query('DELETE FROM etl_control_files');
    console.log(`✅ ${filesResult.rowCount} registros removidos de etl_control_files`);
    
    // Limpar runs
    const runsResult = await client.query('DELETE FROM etl_control_runs');
    console.log(`✅ ${runsResult.rowCount} registros removidos de etl_control_runs`);
    
    // Limpar tabelas de dados (opcional - descomente se quiser recomeçar do zero)
    // await client.query('TRUNCATE TABLE socios CASCADE');
    // await client.query('TRUNCATE TABLE estabelecimentos CASCADE');
    // await client.query('TRUNCATE TABLE municipios CASCADE');
    // console.log('✅ Tabelas de dados truncadas');
    
    console.log('🎉 Limpeza concluída! Você pode rodar o ETL novamente.');
  } catch (error) {
    console.error('❌ Erro:', error.message);
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

limparControle();
