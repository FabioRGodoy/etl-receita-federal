import { pool } from './src/config/database.js';

async function checkConstraints() {
  const client = await pool.connect();
  
  try {
    const result = await client.query(`
      SELECT 
          tc.table_name, 
          tc.constraint_name, 
          tc.constraint_type,
          STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu 
          ON tc.constraint_name = kcu.constraint_name
      WHERE tc.table_schema = 'public'
          AND tc.table_name IN ('estabelecimentos', 'socios', 'municipios')
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
      GROUP BY tc.table_name, tc.constraint_name, tc.constraint_type
      ORDER BY tc.table_name, tc.constraint_type;
    `);
    
    console.log('📋 Constraints criadas:');
    console.table(result.rows);
  } finally {
    client.release();
    await pool.end();
  }
}

checkConstraints();
